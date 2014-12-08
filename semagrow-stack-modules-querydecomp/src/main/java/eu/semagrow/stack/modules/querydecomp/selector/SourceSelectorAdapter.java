package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.utils.resourceselector.ResourceSelector;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;
import eu.semagrow.stack.modules.api.source.SourceMetadata;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import java.util.*;

/**
 * Created by angel on 5/27/14.
 */
@Deprecated
public class SourceSelectorAdapter implements SourceSelector {

    private ResourceSelector resourceSelector;

    public SourceSelectorAdapter(ResourceSelector resourceSelector) {
        this.resourceSelector = resourceSelector;
    }

    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        List<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for (SelectedResource r : resourceSelector.getSelectedResources(pattern,0)) {
            list.add(convert(pattern, r));
        }

        return list;
    }

    public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        List<SourceMetadata> sourceMetadata = new LinkedList<SourceMetadata>();
        for (StatementPattern p : patterns) {
            sourceMetadata.addAll(getSources(p, dataset, bindings));
        }
        return sourceMetadata;
    }

    public List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        if (expr instanceof StatementPattern)
            return getSources((StatementPattern)expr, dataset, bindings);

        return null;
    }

    private SourceMetadata convert(final StatementPattern pattern, final SelectedResource resource) {
        SourceMetadata metadata = new SourceMetadata() {
            public List<URI> getEndpoints() {
                List<URI> endpoints = new ArrayList<URI>(1);
                endpoints.add(resource.getEndpoint());
                return endpoints;
            }

            public StatementPattern original() {
                return pattern;
            }

            public StatementPattern target() {
                return pattern;
            }

            public boolean isTransformed() {
                return false;
            }

            public double getSemanticProximity() {
                return 1;
            }

            public Collection<URI> getSchema(String var) { return Collections.emptySet(); }
        };

        return metadata;
    }
}
