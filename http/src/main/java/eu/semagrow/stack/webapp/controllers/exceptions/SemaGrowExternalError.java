/**
 * 
 */
package eu.semagrow.stack.webapp.controllers.exceptions;

/**
 * @author Giannis Mouchakis
 *
 */
public class SemaGrowExternalError extends SemaGrowException {


        
	/**
	 * 
	 */
	private static final long serialVersionUID = -5459101276974761274L;

	/**
	 * 
	 */
	public SemaGrowExternalError() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public SemaGrowExternalError(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SemaGrowExternalError(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public SemaGrowExternalError(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public SemaGrowExternalError(Throwable cause) {
		super(cause);
	}

	
	
}
