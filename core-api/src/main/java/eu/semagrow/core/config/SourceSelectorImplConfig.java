package eu.semagrow.core.config;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;

/**
 * @author Angelos Charalambidis
 */
public interface SourceSelectorImplConfig {

    String getType();

    void validate() throws SourceSelectorConfigException;

    Resource export(Graph graph);

    void parse(Graph graph, Resource resource) throws SourceSelectorConfigException;

}
