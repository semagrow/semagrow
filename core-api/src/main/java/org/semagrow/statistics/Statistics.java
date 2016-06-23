package org.semagrow.statistics;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

/**
 * A simple interface that provides statistics for tuples in a triple store
 * identified by its endpoint
 * @author Angelos Charalambidis
 */
public interface Statistics {

    long getTriplesCount();

    StatsItem getStats(StatementPattern pattern, BindingSet bindings);
}
