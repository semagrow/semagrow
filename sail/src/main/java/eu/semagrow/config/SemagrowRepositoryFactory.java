package eu.semagrow.config;

import eu.semagrow.repository.impl.SemagrowSailRepository;
import eu.semagrow.sail.SemagrowSail;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailRegistry;

import java.util.Optional;

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
        Optional<SailFactory> sailFactory = SailRegistry.getInstance().get(config.getSemagrowSailConfig().getType());
        if (sailFactory.isPresent()) {

            try {

                SemagrowSail sail = (SemagrowSail) sailFactory.get().getSail(config.getSemagrowSailConfig());
                return new SemagrowSailRepository(sail);
            } catch (SailConfigException e) {
                throw new RepositoryConfigException(e);
            }
        }

        throw new RepositoryConfigException();
    }
}
