package eu.semagrow.stack.modules.sails.semagrow.config;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;

/**
 * Created by angel on 11/1/14.
 */
public interface SourceSelectorImplConfig {

    String getType();

    void validate() throws SourceSelectorConfigException;

    Resource export(Graph graph);

    void parse(Graph graph, Resource resource) throws SourceSelectorConfigException;

}
