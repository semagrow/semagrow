package eu.semagrow.core.impl.selector;

import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.core.source.SourceSelector;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by angel on 15/6/2015.
 */
public class CollectionSourceSelector implements SourceSelector
{
    private Set<IRI> endpoints;

    public CollectionSourceSelector(Collection<IRI> uriCollection) {
        this.endpoints = new HashSet<>(uriCollection);
    }

    public CollectionSourceSelector() {
        this.endpoints = new HashSet<>();
    }

    public void addSource(IRI source) {
        this.endpoints.add(source);
    }

    @Override
    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return null;
    }

    @Override
    public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        return null;
    }

    @Override
    public List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        return null;
    }

}
