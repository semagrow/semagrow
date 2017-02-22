package org.semagrow.selector;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.Collection;


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

    void setSiteResolver(SiteResolver siteResolver);

    /**
     * Returns a list of operational endpoints where you can find
     * triples that match the given pattern.
     * @param pattern
     * @return a list of endpoints
     */
    Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings);

    /**
     * Returns a list of operational endpoints where you can find
     * triples that match the given expression.
     * @param expr
     * @param dataset
     * @param bindings
     * @return
     */
    Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings);

}
