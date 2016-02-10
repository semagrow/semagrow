package eu.semagrow.config;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryImplConfigBase;
import org.openrdf.sail.config.SailConfigException;

import static org.openrdf.repository.sail.config.SailRepositorySchema.SAILIMPL;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowRepositoryConfig extends RepositoryImplConfigBase {

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
    public Resource export(Graph graph) {
        Resource repImplNode = super.export(graph);

        if (sailConfig != null) {
            Resource sailImplNode = sailConfig.export(graph);
            graph.add(repImplNode, SAILIMPL, sailImplNode);
        }

        return repImplNode;
    }


    @Override
    public void parse(Graph graph, Resource node) throws RepositoryConfigException {

        try {
            Resource sailImplNode = GraphUtil.getOptionalObjectResource(graph, node, SAILIMPL);

            if (sailImplNode != null) {

                    sailConfig  = new SemagrowSailConfig();
                    sailConfig.parse(graph, sailImplNode);
            }
        }
        catch (GraphUtilException e) {
            throw new RepositoryConfigException(e.getMessage(), e);
        }
        catch (SailConfigException e) {
            throw new RepositoryConfigException(e.getMessage(), e);
        }
    }
}