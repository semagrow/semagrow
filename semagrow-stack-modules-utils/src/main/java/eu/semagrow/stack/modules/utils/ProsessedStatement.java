package eu.semagrow.stack.modules.utils;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;

/**
 * Object used to hold the subject, predicate and object of a {@link StatementPattern} if instanceof {@link URI}, else null. 
 * 
 * @author Giannis Mouchakis
 *
 */
public interface ProsessedStatement {

	/**
	 * @return the subject
	 */
	public URI getSubject();

	/**
	 * @param subject the subject to set
	 */
	public void setSubject(URI subject);

	/**
	 * @return the object
	 */
	public URI getObject();

	/**
	 * @param object the object to set
	 */
	public void setObject(URI object);

	/**
	 * @return the predicate
	 */
	public URI getPredicate();

	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(URI predicate);

	/**
	 * @return true if no URIs found else false
	 */
	public boolean noURIsFound();

}