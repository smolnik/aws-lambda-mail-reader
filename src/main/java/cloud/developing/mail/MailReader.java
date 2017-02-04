package cloud.developing.mail;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.mail.Session;
import javax.mail.internet.MimeMessage;

import org.apache.commons.mail.util.MimeMessageParser;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.polly.AmazonPolly;
import com.amazonaws.services.polly.AmazonPollyClient;
import com.amazonaws.services.polly.model.DescribeVoicesRequest;
import com.amazonaws.services.polly.model.OutputFormat;
import com.amazonaws.services.polly.model.VoiceId;
import com.amazonaws.services.polly.presign.SynthesizeSpeechPresignRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.AmazonSNSException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;

import cloud.developing.exception.MailReaderException;

/**
 * @author asmolnik
 *
 */
public class MailReader {

	private static class VoiceUrls {

		private final String fromMp3Url, subjectMp3Url, contentMp3Url;

		private VoiceUrls(String fromMp3Url, String subjectMp3Url, String contentMp3Url) {
			this.fromMp3Url = fromMp3Url;
			this.subjectMp3Url = subjectMp3Url;
			this.contentMp3Url = contentMp3Url;
		}

	}

	private static final int MAX_CONTENT_LENGTH = 500;

	private static final String DEFAULT_LANGUAGE = "en";

	private static final String S3_MAIL_READER_PREFIX = "https://s3.amazonaws.com/mail-reader";

	private static final String NEW_MESSAGE_MP3_URL = S3_MAIL_READER_PREFIX + "/new-message.mp3";

	private static final String MISSING_SUBJECT = S3_MAIL_READER_PREFIX + "/missing-subject.mp3";

	private static final String MISSING_CONTENT = S3_MAIL_READER_PREFIX + "/missing-content.mp3";

	private static final String CONTENT_ISSUE = S3_MAIL_READER_PREFIX + "/content-issue.mp3";

	private static final String MAIL_ISSUE = S3_MAIL_READER_PREFIX + "/mail-issue.mp3";

	private final String mobileAppTopic = System.getenv("mail_reader_mobile_app_topic");

	private final DynamoDB db = new DynamoDB(new AmazonDynamoDBClient());

	private final AmazonSNS sns = new AmazonSNSClient();

	private final AmazonPolly polly = new AmazonPollyClient();

	private final AmazonS3 s3 = new AmazonS3Client();

	private final VoiceId defaultVoiceId = VoiceId.Joanna;

	private final List<LanguageProfile> languageProfiles;

	{

		try {
			languageProfiles = new LanguageProfileReader().readAllBuiltIn();
		} catch (IOException e) {
			throw new MailReaderException(e);
		}

	}

	private final LanguageDetector ld = LanguageDetectorBuilder.create(NgramExtractors.standard()).withProfiles(languageProfiles).build();

	private final ConcurrentMap<String, VoiceId> voiceIdMap = new ConcurrentHashMap<>();

	{

		polly.describeVoices(new DescribeVoicesRequest()).getVoices().forEach(v -> {
			voiceIdMap.put(v.getLanguageCode().split("-")[0], VoiceId.fromValue(v.getId()));
		});

	}

	public void process(S3Event event, Context context) {
		LambdaLogger log = context.getLogger();
		log.log(event.toJson());
		event.getRecords().forEach(r -> {
			S3Entity s3Entity = r.getS3();
			String bucket = s3Entity.getBucket().getName();
			String key = s3Entity.getObject().getKey();
			String user = key.split("/")[1];
			log.log("user: " + user);
			Item userData = db.getTable("mail-reader-users").getItem("user", user);
			String mobileEndpoint = mobileAppTopic + "/" + userData.getString("deviceId");
			try {
				VoiceUrls vus = processesMail(bucket, key);
				String urls = String.format("%s|%s|%s|%s", NEW_MESSAGE_MP3_URL, vus.fromMp3Url, vus.subjectMp3Url, vus.contentMp3Url);
				log.log("urls as sns message: " + urls);
				log.log("mobileEndpoint: " + mobileEndpoint);
				try {
					sendToSns(urls, mobileEndpoint);
				} catch (AmazonSNSException e) {
					log.log(exceptionToString(e));
					sendToSns(String.format("%s|%s|%s|%s", NEW_MESSAGE_MP3_URL, vus.fromMp3Url, vus.subjectMp3Url, CONTENT_ISSUE), mobileEndpoint);
				}
			} catch (RuntimeException re) {
				log.log(exceptionToString(re));
				sendToSns(String.format("%s|%s", NEW_MESSAGE_MP3_URL, MAIL_ISSUE), mobileEndpoint);
			}
		});
	}

	private void sendToSns(String message, String mobileEndpoint) {
		sns.publish(new PublishRequest().withTargetArn(mobileEndpoint).withMessage(message));
	}

	private String exceptionToString(Exception e) {
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		return errors.toString();
	}

	private VoiceUrls processesMail(String mailBucket, String mailKey) {
		try (InputStream is = s3.getObject(mailBucket, mailKey).getObjectContent()) {
			MimeMessageParser parser = new MimeMessageParser(new MimeMessage(Session.getDefaultInstance(new Properties()), is));
			parser.parse();
			String from = parser.getFrom();
			String subject = parser.getSubject();
			String plainContent = parser.getPlainContent();
			String content = trimAndSanitizeContent(plainContent).orNull();
			String lang = detectLanguageBasedOn(subject, content);
			String fromMp3Url = generateMp3Url(from, lang).get();
			String subjectMp3Url = generateMp3Url(subject, lang).or(MISSING_SUBJECT);
			String contentMp3Url = generateMp3Url(content, lang).or(MISSING_CONTENT);
			return new VoiceUrls(fromMp3Url, subjectMp3Url, contentMp3Url);
		} catch (Exception e) {
			throw new MailReaderException(e);
		}
	}

	private Optional<String> generateMp3Url(String text, String lang) {
		if (isEmpty(text) || isEmpty(lang)) {
			return Optional.absent();
		}
		return Optional.of(polly.presigners().getPresignedSynthesizeSpeechUrl(new SynthesizeSpeechPresignRequest().withText(text)
				.withVoiceId(voiceIdMap.getOrDefault(lang, defaultVoiceId)).withOutputFormat(OutputFormat.Mp3)).toString());
	}

	private Optional<LdLocale> detect(String content) {
		return !isEmpty(content) ? ld.detect(content) : Optional.absent();
	}

	private Optional<String> trimAndSanitizeContent(String content) {
		if (isEmpty(content)) {
			return Optional.absent();
		}

		content = content.replaceAll("((mailto:|(news|(ht|f)tp(s?))://){1}\\S+)|\\[|\\]|", "").trim();
		if (content.length() <= MAX_CONTENT_LENGTH) {
			return Optional.of(content);
		}
		String[] ss = content.substring(0, MAX_CONTENT_LENGTH).split("\\b");
		return Optional.of(content.substring(0, MAX_CONTENT_LENGTH - ss[ss.length - 1].length()));
	}

	private String detectLanguageBasedOn(String subject, String plainContent) {
		Optional<LdLocale> lpc = detect(plainContent);
		if (lpc.isPresent()) {
			lpc.get();
		}
		Optional<LdLocale> ls = detect(subject);
		return ls.isPresent() ? ls.get().getLanguage() : DEFAULT_LANGUAGE;
	}

	private boolean isEmpty(String s) {
		return s == null || s.trim().isEmpty();
	}

}
