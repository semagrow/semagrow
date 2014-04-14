/**
 * 
 */
package eu.semagrow.stack.modules.utils.resourceselector.impl;

import org.openrdf.model.URI;

import eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement;

/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement
 */
public class ProcessedStatementImpl implements ProsessedStatement {
	
	private URI subject;
	private URI object;
	private URI predicate;
	
	/**
	 * 
	 */
	public ProcessedStatementImpl() {
		super();
		this.subject = null;
		this.object = null;
		this.predicate = null;
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement#getSubject()
	 */
	public URI getSubject() {
		return subject;
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement#setSubject(org.openrdf.model.URI)
	 */
	public void setSubject(URI subject) {
		this.subject = subject;
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement#getObject()
	 */
	public URI getObject() {
		return object;
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement#setObject(org.openrdf.model.URI)
	 */
	public void setObject(URI object) {
		this.object = object;
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement#getPredicate()
	 */
	public URI getPredicate() {
		return predicate;
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement#setPredicate(org.openrdf.model.URI)
	 */
	public void setPredicate(URI predicate) {
		this.predicate = predicate;
	}
	
	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.ProsessedStatement#noURIsFound()
	 */
	public boolean noURIsFound() {
		if (subject == null && predicate == null && object == null) {
			return true;
		} else {
			return false;
		}
	}

	
}
