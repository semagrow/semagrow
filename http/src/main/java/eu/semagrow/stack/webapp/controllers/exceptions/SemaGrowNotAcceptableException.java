/**
 * 
 */
package eu.semagrow.stack.webapp.controllers.exceptions;

import javax.servlet.http.HttpServletResponse;

/**
 * @author Giannis Mouchakis
 *
 */
public class SemaGrowNotAcceptableException extends SemaGrowException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6054327542082741039L;

	/**
	 * 
	 */
	public SemaGrowNotAcceptableException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public SemaGrowNotAcceptableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SemaGrowNotAcceptableException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public SemaGrowNotAcceptableException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public SemaGrowNotAcceptableException(Throwable cause) {
		super(cause);
	}

        @Override
        public int getResponseCode() {
            return HttpServletResponse.SC_NOT_ACCEPTABLE;
        }

}
