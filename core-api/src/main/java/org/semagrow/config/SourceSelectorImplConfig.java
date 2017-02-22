package org.semagrow.config;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

/**
 * @author Angelos Charalambidis
 */
public interface SourceSelectorImplConfig {

    String getType();

    void validate() throws SourceSelectorConfigException;

    Resource export(Model graph);

    void parse(Model graph, Resource resource) throws SourceSelectorConfigException;

}
