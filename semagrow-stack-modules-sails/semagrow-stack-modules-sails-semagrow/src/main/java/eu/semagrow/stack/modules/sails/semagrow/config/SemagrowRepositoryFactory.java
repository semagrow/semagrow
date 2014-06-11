package eu.semagrow.stack.modules.sails.semagrow.config;

import eu.semagrow.stack.modules.sails.semagrow.SemagrowRepository;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSail;
import org.openrdf.repository.Repository;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailRegistry;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowRepositoryFactory implements RepositoryFactory {

    public static final String REPOSITORY_TYPE = "semagrow:SemagrowRepository";

    public String getRepositoryType() {
        return REPOSITORY_TYPE;
    }

    public RepositoryImplConfig getConfig() {
        return new SemagrowRepositoryConfig();
    }

    public Repository getRepository(RepositoryImplConfig repositoryImplConfig)
            throws RepositoryConfigException {

        assert repositoryImplConfig instanceof SemagrowRepositoryConfig;

        SemagrowRepositoryConfig config = (SemagrowRepositoryConfig) repositoryImplConfig;
        SailFactory sailFactory = SailRegistry.getInstance().get(config.getSemagrowSailConfig().getType());
        try {
            SemagrowSail sail = (SemagrowSail) sailFactory.getSail(config.getSemagrowSailConfig());
            return new SemagrowRepository(sail);
        } catch (SailConfigException e) {
            throw new RepositoryConfigException(e);
        }
    }
}
