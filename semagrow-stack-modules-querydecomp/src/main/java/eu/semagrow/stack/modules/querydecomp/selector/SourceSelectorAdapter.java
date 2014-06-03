package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.api.SelectedResource;
import eu.semagrow.stack.modules.querydecomp.SourceMetadata;
import eu.semagrow.stack.modules.querydecomp.SourceSelector;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by angel on 5/27/14.
 */
@Deprecated
public class SourceSelectorAdapter implements SourceSelector{

    private ResourceSelector resourceSelector;

    public SourceSelectorAdapter(ResourceSelector resourceSelector) {
        this.resourceSelector = resourceSelector;
    }


    public List<SourceMetadata> getSources(StatementPattern pattern) {
        List<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for (SelectedResource r : resourceSelector.getSelectedResources(pattern,0)) {
            list.add(convert(pattern, r));
        }

        return list;
    }

    private SourceMetadata convert(final StatementPattern pattern, final SelectedResource resource) {
        SourceMetadata metadata = new SourceMetadata() {
            public List<URI> getEndpoints() {
                List<URI> endpoints = new ArrayList<URI>(1);
                endpoints.add(resource.getEndpoint());
                return endpoints;
            }

            public StatementPattern originalPattern() {
                return pattern;
            }

            public boolean requiresTransform() {
                return false;
            }

            public double getSemanticProximity() {
                return 1;
            }
        };

        return metadata;
    }
}
