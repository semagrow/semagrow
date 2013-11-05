/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.net.URI;

/**
 * @author Giannis Mouchakis
 *
 */
public class ProcessedStatement {
	
	private URI subject;
	private URI object;
	private URI predicate;
	
	/**
	 * 
	 */
	public ProcessedStatement() {
		super();
		this.subject = null;
		this.object = null;
		this.predicate = null;
	}

	/**
	 * @return the subject
	 */
	public URI getSubject() {
		return subject;
	}

	/**
	 * @param subject the subject to set
	 */
	public void setSubject(URI subject) {
		this.subject = subject;
	}

	/**
	 * @return the object
	 */
	public URI getObject() {
		return object;
	}

	/**
	 * @param object the object to set
	 */
	public void setObject(URI object) {
		this.object = object;
	}

	/**
	 * @return the predicate
	 */
	public URI getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(URI predicate) {
		this.predicate = predicate;
	}
	
	/**
	 * @return true if no URIs found else false
	 */
	public boolean noURIsFound() {
		if (subject == null && predicate == null && object == null) {
			return true;
		} else {
			return false;
		}
	}

	
}
