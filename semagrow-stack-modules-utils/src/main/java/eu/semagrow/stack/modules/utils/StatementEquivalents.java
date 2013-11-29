package eu.semagrow.stack.modules.utils;

import java.util.ArrayList;

import org.openrdf.query.algebra.StatementPattern;

/**
 * Object to hold lists of {@link EquivalentURI} for the subject, predicate and
 * object of a {@link StatementPattern}. If no equivalents where found or if the
 * {@link StatementPattern} does not contain URI then these list are empty.
 * 
 * @author Giannis Mouchakis
 * 
 */
public interface StatementEquivalents {

	/**
	 * @return the subject_equivalents
	 */
	public ArrayList<EquivalentURI> getSubject_equivalents();

	/**
	 * @param subject_equivalents the subject_equivalents to set
	 */
	public void setSubject_equivalents(
			ArrayList<EquivalentURI> subject_equivalents);

	/**
	 * @return the predicate_equivalents
	 */
	public ArrayList<EquivalentURI> getPredicate_equivalents();

	/**
	 * @param predicate_equivalents the predicate_equivalents to set
	 */
	public void setPredicate_equivalents(
			ArrayList<EquivalentURI> predicate_equivalents);

	/**
	 * @return the object_equivalents
	 */
	public ArrayList<EquivalentURI> getObject_equivalents();

	/**
	 * @param object_equivalents the object_equivalents to set
	 */
	public void setObject_equivalents(
			ArrayList<EquivalentURI> object_equivalents);

}