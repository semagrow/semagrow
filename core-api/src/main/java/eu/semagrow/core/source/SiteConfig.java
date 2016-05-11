package eu.semagrow.core.source;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;

/**
 * Created by angel on 5/4/2016.
 */
public interface SiteConfig {

    String getType();

    void validate();

    Resource export(Graph graph);

    void parse(Graph graph, Resource resource);

}
