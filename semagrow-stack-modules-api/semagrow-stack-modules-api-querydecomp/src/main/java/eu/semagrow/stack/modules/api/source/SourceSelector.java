package eu.semagrow.stack.modules.api.source;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import java.util.List;

/**
 * @author Angelos Charalambidis
 */
public interface SourceSelector {

    /**
     * Returns a list of operational endpoints where you can find
     * triples that match the given pattern.
     * @param pattern
     * @return a list of endpoints
     */
    List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings);

    List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings);

    List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings);

}
