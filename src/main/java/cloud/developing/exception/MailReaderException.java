package cloud.developing.exception;

/**
 * @author asmolnik
 *
 */
public class MailReaderException extends RuntimeException {

	private static final long serialVersionUID = 7304522865359256868L;

	public MailReaderException(Throwable t) {
		super(t);
	}

}
