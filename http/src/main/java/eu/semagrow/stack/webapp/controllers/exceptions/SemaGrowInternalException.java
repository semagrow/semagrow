package eu.semagrow.stack.webapp.controllers.exceptions;

public class SemaGrowInternalException extends SemaGrowException {



	/**
	 * 
	 */
	private static final long serialVersionUID = 6997670753157036173L;

	/**
	 * 
	 */
	public SemaGrowInternalException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public SemaGrowInternalException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SemaGrowInternalException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public SemaGrowInternalException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public SemaGrowInternalException(Throwable cause) {
		super(cause);
	}

}
