package eu.semagrow.core.statistics;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

/**
 * A simple interface that provides statistics for tuples in a triple store
 * identified by its endpoint
 * @author Angelos Charalambidis
 */
public interface StatisticsProvider {

    long getTriplesCount(IRI source);

    /*
    long getPatternCount(StatementPattern pattern, URI source);

    long getDistinctObjects(StatementPattern pattern, URI source);

    long getDistinctSubjects(StatementPattern pattern, URI source);

    long getDistinctPredicates(StatementPattern pattern, URI source);
    */

    Statistics getStats(StatementPattern pattern, BindingSet bindings, IRI source);

}
