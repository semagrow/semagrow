package org.semagrow.repository.config;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.repository.config.AbstractRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.semagrow.sail.config.SemagrowSailConfig;

import java.util.Optional;

import static org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema.SAILIMPL;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowRepositoryConfig extends AbstractRepositoryImplConfig {

    private SemagrowSailConfig sailConfig = new SemagrowSailConfig();

    public SemagrowRepositoryConfig() {
        super(SemagrowRepositoryFactory.REPOSITORY_TYPE);
    }

    public SemagrowSailConfig getSemagrowSailConfig() {
        return sailConfig;
    }

    public void setSemagrowSailConfig(SemagrowSailConfig config) {
        sailConfig = config;
    }

    @Override
    public Resource export(Model graph) {
        Resource repImplNode = super.export(graph);

        if (sailConfig != null) {
            Resource sailImplNode = sailConfig.export(graph);
            graph.add(repImplNode, SAILIMPL, sailImplNode);
        }

        return repImplNode;
    }


    @Override
    public void parse(Model graph, Resource node) throws RepositoryConfigException {

        try {
            Optional<Resource> sailImplNode = Models.objectResource(graph.filter(node, SAILIMPL,null));

            if (sailImplNode.isPresent()) {

                    sailConfig  = new SemagrowSailConfig();
                    sailConfig.parse(graph, sailImplNode.get());
            }
        }
        catch (SailConfigException e) {
            throw new RepositoryConfigException(e.getMessage(), e);
        }
    }
}