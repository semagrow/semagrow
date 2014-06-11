package eu.semagrow.stack.modules.utils.resourceselector;

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
	 * @return the  source endpoint that holds triples for this pattern
	 */
	public URI getEndpoint();

	/**
	 * @return the volume of the triples
	 */
	public int getVol();

	/**
	 * @return the variety of the triples
	 */
	public int getVar();

	/**
	 * @return the equivalent subject of the candidate query pattern (if any, else null).
	 */
	public URI getSubject();

	/**
	 * @param subject the equivalent subject of the candidate query pattern (if any, else null) to set
	 */
	public void setSubject(URI subject);

	/**
	 * @return the proximity of the equivalent subject to the original URI (if any, else -1).
	 */
	public int getSubjectProximity();

	/**
	 * @param subjectProximity the proximity of the equivalent subject to the original URI (if any, else -1) to set
	 */
	public void setSubjectProximity(int subjectProximity);

	/**
	 * @return the equivalent predicate of the candidate query pattern (if any, else null).
	 */
	public URI getPredicate();

	/**
	 * @param predicate the equivalent predicate of the candidate query pattern (if any, else null) to set
	 */
	public void setPredicate(URI predicate);

	/**
	 * @return the proximity of the equivalent predicate to the original URI (if any, else -1).
	 */
	public int getPredicateProximity();

	/**
	 * @param predicateProximity the proximity of the equivalent predicate to the original URI (if any, else -1)
	 */
	public void setPredicateProximity(int predicateProximity);

	/**
	 * @return the equivalent object of the candidate query pattern (if any, else null).
	 */
	public URI getObject();

	/**
	 * @param object the equivalent object of the candidate query pattern (if any, else null) to set
	 */
	public void setObject(URI object);

	/**
	 * @return the proximity of the equivalent object to the original URI (if any, else -1).
	 */
	public int getObjectProximity();

	/**
	 * @param objectProximity the proximity of the equivalent object to the original URI (if any, else -1) to set
	 */
	public void setObjectProximity(int objectProximity);

	/**
	 * @return the load info of the source
	 */
	public List<Measurement> getLoadInfo();

	/**
	 * @param loadInfo the load info of the source to set
	 */
	public void setLoadInfo(List<Measurement> loadInfo);


}