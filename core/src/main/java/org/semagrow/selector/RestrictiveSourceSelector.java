package org.semagrow.selector;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.*;

/**
 * Created by angel on 6/19/14.
 */
public class RestrictiveSourceSelector extends SourceSelectorWrapper {

    private Set<IRI> includeOnly;

    private Set<IRI> exclude;

    public RestrictiveSourceSelector(SourceSelector selector) {
        super(selector);
        includeOnly = new HashSet<IRI>();
        exclude = new HashSet<IRI>();
    }

    public RestrictiveSourceSelector(SourceSelector selector,
                                     Collection<IRI> includeOnly,
                                     Collection<IRI> exclude)
    {
        this(selector);
        includeOnlySources(includeOnly);
        excludeSources(exclude);
    }

    public void excludeSources(Collection<IRI> sources) { exclude.addAll(sources); }

    public void includeOnlySources(Collection<IRI> sources) { includeOnly.addAll(sources); }

    public boolean isRestrictive() { return !(includeOnly.isEmpty() && exclude.isEmpty()); }

    @Override
    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        Collection<SourceMetadata> list = super.getSources(pattern, dataset, bindings);
        return isRestrictive() ? restrictSourceList(list) : list;
    }

    @Override
    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {

        Collection<SourceMetadata> res = getWrappedSelector().getSources(expr, dataset, bindings);
        return isRestrictive() ? restrictSourceList(res) : res;
    }

    private Collection<SourceMetadata> restrictSourceList(Collection<SourceMetadata> list) {
        Collection<SourceMetadata> restrictedList = new LinkedList<SourceMetadata>();

        for (SourceMetadata metadata : list) {
            Collection<Site> metadataSources = metadata.getSites();
            if (exclude.isEmpty() &&
                Collections.disjoint(metadataSources, exclude))
            {

                if (includeOnly.isEmpty() ||
                    !Collections.disjoint(metadataSources, includeOnly))
                {
                    SourceMetadata m;
                    if (includeOnly.isEmpty()) {
                        m = metadata;
                    } else {
                        m = new IncludeOnlySourceMetadata(metadata, includeOnly);
                    }
                    restrictedList.add(m);
                }
            }
        }
        return restrictedList;
    }

    protected class IncludeOnlySourceMetadata implements SourceMetadata {

        private final SourceMetadata metadata;
        private final Set<IRI> includeOnly;

        public IncludeOnlySourceMetadata(final SourceMetadata metadata, final Set<IRI> includeOnly) {
            this.metadata = metadata;
            this.includeOnly = includeOnly;
        }

        public List<Site> getSites() {
            List<Site> l =  new LinkedList<Site>(metadata.getSites());
            l.retainAll(includeOnly);
            return l;
        }

        public StatementPattern original() { return metadata.original(); }

        public StatementPattern target() { return metadata.target(); }

        public Collection<IRI> getSchema(String var) { return metadata.getSchema(var); }

        public boolean isTransformed() { return metadata.isTransformed(); }

        public double getSemanticProximity() { return metadata.getSemanticProximity(); }
    }
}
