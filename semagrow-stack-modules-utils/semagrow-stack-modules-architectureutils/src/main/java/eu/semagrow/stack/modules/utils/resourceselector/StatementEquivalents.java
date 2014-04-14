package eu.semagrow.stack.modules.utils.resourceselector;

import java.util.List;

import org.openrdf.query.algebra.StatementPattern;

/**
 * Object to hold lists of {@link EquivalentURI}s for the subject, predicate and
 * object of a {@link StatementPattern}. If no equivalents where found or if the
 * {@link StatementPattern} does not contain URI then these lists are empty.
 * 
 * @author Giannis Mouchakis
 * 
 */
public interface StatementEquivalents {

	/**
	 * @return the subject_equivalents
	 */
	public List<EquivalentURI> getSubject_equivalents();

	/**
	 * @param subject_equivalents the subject_equivalents to set
	 */
	public void setSubject_equivalents(
			List<EquivalentURI> subject_equivalents);

	/**
	 * @return the predicate_equivalents
	 */
	public List<EquivalentURI> getPredicate_equivalents();

	/**
	 * @param predicate_equivalents the predicate_equivalents to set
	 */
	public void setPredicate_equivalents(
			List<EquivalentURI> predicate_equivalents);

	/**
	 * @return the object_equivalents
	 */
	public List<EquivalentURI> getObject_equivalents();

	/**
	 * @param object_equivalents the object_equivalents to set
	 */
	public void setObject_equivalents(
			List<EquivalentURI> object_equivalents);

}