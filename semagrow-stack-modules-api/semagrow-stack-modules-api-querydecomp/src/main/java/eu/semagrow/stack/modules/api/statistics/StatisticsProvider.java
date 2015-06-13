package eu.semagrow.stack.modules.api.statistics;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;

/**
 * A simple interface that provides statistics for tuples in a triple store
 * identified by its endpoint
 * @author Angelos Charalambidis
 */
public interface StatisticsProvider {

    long getTriplesCount(URI source);

    /*
    long getPatternCount(StatementPattern pattern, URI source);

    long getDistinctObjects(StatementPattern pattern, URI source);

    long getDistinctSubjects(StatementPattern pattern, URI source);

    long getDistinctPredicates(StatementPattern pattern, URI source);
    */

    Statistics getStats(StatementPattern pattern, BindingSet bindings, URI source);

}
