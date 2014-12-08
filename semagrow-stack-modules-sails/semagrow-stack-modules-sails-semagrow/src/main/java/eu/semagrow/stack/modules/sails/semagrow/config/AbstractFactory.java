package eu.semagrow.stack.modules.sails.semagrow.config;

import org.openrdf.repository.Repository;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailImplConfig;

/**
 * Created by angel on 11/2/14.
 */
public class AbstractFactory {


    public Sail createSail(SailImplConfig config) {
        return null;
    }

    public Repository createRepository(RepositoryImplConfig config) {
        return null;
    }
}
