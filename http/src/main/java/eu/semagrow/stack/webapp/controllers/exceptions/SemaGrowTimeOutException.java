/**
 * 
 */
package eu.semagrow.stack.webapp.controllers.exceptions;

import javax.servlet.http.HttpServletResponse;

/**
 * @author Giannis Mouchakis
 *
 */
public class SemaGrowTimeOutException extends SemaGrowException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -744808877090137431L;

	/**
	 * 
	 */
	public SemaGrowTimeOutException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public SemaGrowTimeOutException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SemaGrowTimeOutException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public SemaGrowTimeOutException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public SemaGrowTimeOutException(Throwable cause) {
		super(cause);
	}

        @Override
        public int getResponseCode() {
            return HttpServletResponse.SC_GATEWAY_TIMEOUT;
        }

}
