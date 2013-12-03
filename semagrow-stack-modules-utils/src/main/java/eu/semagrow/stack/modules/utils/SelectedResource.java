package eu.semagrow.stack.modules.utils;

import java.util.List;

import org.openrdf.model.URI;

/**
 * This class is used to represent a {@link ResourceSelector} result. It
 * contains a combination of equivalent URIs that make up a query pattern, their
 * proximity to the original URIs, the source endpoint that holds triples for
 * this pattern, the predicted volume of the results, the variety and load info
 * of the endpoint.
 * 
 * @author Giannis Mouchakis
 * 
 */
public interface SelectedResource {

	/**
	 * @return the endpoint
	 */
	public URI getEndpoint();

	/**
	 * @return the vol
	 */
	public int getVol();

	/**
	 * @return the var
	 */
	public int getVar();

	/**
	 * @return the subject
	 */
	public URI getSubject();

	/**
	 * @param subject the subject to set
	 */
	public void setSubject(URI subject);

	/**
	 * @return the subjectProximity
	 */
	public int getSubjectProximity();

	/**
	 * @param subjectProximity the subjectProximity to set
	 */
	public void setSubjectProximity(int subjectProximity);

	/**
	 * @return the predicate
	 */
	public URI getPredicate();

	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(URI predicate);

	/**
	 * @return the predicateProximity
	 */
	public int getPredicateProximity();

	/**
	 * @param predicateProximity the predicateProximity to set
	 */
	public void setPredicateProximity(int predicateProximity);

	/**
	 * @return the object
	 */
	public URI getObject();

	/**
	 * @param object the object to set
	 */
	public void setObject(URI object);

	/**
	 * @return the objectProximity
	 */
	public int getObjectProximity();

	/**
	 * @param objectProximity the objectProximity to set
	 */
	public void setObjectProximity(int objectProximity);

	/**
	 * @return the loadInfo
	 */
	public List<Measurement> getLoadInfo();

	/**
	 * @param loadInfo the loadInfo to set
	 */
	public void setLoadInfo(List<Measurement> loadInfo);

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString();

}