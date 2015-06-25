/**
 * 
 */
package eu.semagrow.stack.webapp.controllers.exceptions;

import javax.servlet.http.HttpServletResponse;

/**
 * @author Giannis Mouchakis
 *
 */
public class SemaGrowBadRequestException extends SemaGrowException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6590845574714848697L;

	/**
	 * 
	 */
	public SemaGrowBadRequestException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public SemaGrowBadRequestException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SemaGrowBadRequestException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public SemaGrowBadRequestException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public SemaGrowBadRequestException(Throwable cause) {
		super(cause);
	}

        @Override
        public int getResponseCode() {
            return HttpServletResponse.SC_BAD_REQUEST;
        }

}
