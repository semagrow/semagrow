package eu.semagrow.stack.modules.sails.semagrow.config;

import org.openrdf.repository.config.RepositoryImplConfigBase;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowRepositoryConfig extends RepositoryImplConfigBase {

    @Override
    public String getType() { return SemagrowRepositoryFactory.REPOSITORY_TYPE; }

    public SemagrowConfig getSemagrowSailConfig() { return new SemagrowConfig(); }
}
