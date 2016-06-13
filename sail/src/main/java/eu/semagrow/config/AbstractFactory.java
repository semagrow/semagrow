package eu.semagrow.config;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

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
