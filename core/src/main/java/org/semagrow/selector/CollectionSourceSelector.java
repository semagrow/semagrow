package org.semagrow.selector;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 15/6/2015.
 */
public class CollectionSourceSelector implements SourceSelector
{
    private Set<Site> sites;

    public CollectionSourceSelector(Collection<Site> sites) {
        this.sites = new HashSet<>(sites);
    }

    @Override
    public void setSiteResolver(SiteResolver siteResolver) {

    }

    @Override
    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return null;
    }

    @Override
    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        return null;
    }

}
