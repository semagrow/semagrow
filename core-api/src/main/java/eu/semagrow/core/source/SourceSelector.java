package eu.semagrow.core.source;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import java.util.List;

/**
 * Source Selector
 * 
 * Interface for all components that select data sources that contain triples
 * that match a simple query pattern or a complex expression.
 * 
 * @author Angelos Charalambidis
 */

public interface SourceSelector
{

    /**
     * Returns a list of operational endpoints where you can find
     * triples that match the given pattern.
     * @param pattern
     * @return a list of endpoints
     */
    List<SourceMetadata> getSources( StatementPattern pattern, Dataset dataset, BindingSet bindings );

    /**
     * Returns a list of operational endpoints where you can find
     * triples that match the given patterns.
     * @param patterns
     * @param dataset
     * @param bindings
     * @return
     */
    List<SourceMetadata> getSources( Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings );

    /**
     * Returns a list of operational endpoints where you can find
     * triples that match the given expression.
     * @param expr
     * @param dataset
     * @param bindings
     * @return
     */
    List<SourceMetadata> getSources( TupleExpr expr, Dataset dataset, BindingSet bindings );

}
