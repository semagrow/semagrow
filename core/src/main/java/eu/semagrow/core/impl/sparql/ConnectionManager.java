package eu.semagrow.core.impl.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by angel on 9/7/2015.
 */
public class ConnectionManager {

    protected final Logger logger = LoggerFactory.getLogger(QueryExecutorImpl.class);
    private Map<IRI,Repository> repoMap = new HashMap<IRI,Repository>();
    private Integer countconn = 0;

    public void initialize() { }

    public void shutdown() {
        for (Repository repo : repoMap.values()) {
            if (repo.isInitialized())
                try {
                    repo.shutDown();
                } catch (RepositoryException e) {
                    logger.warn("Failed to shutdown repo {}", repo);
                }
        }
    }

    public RepositoryConnection getConnection(IRI endpoint) throws RepositoryException {
        Repository repo = null;

        if (!repoMap.containsKey(endpoint)) {
            repo = new SPARQLRepository(endpoint.stringValue());
            repoMap.put(endpoint,repo);
        } else {
            repo = repoMap.get(endpoint);
        }

        if (!repo.isInitialized())
            repo.initialize();

        RepositoryConnection conn = repo.getConnection();
        logger.info("Open [{}] (currently open={})", conn, countconn);
        synchronized(this) { countconn++; }
        return conn;
    }

    public void closeQuietly(RepositoryConnection conn) {
        try {
            if (conn.isOpen()) {
                conn.close();
                synchronized (this) { countconn--; }
                logger.info("Close [{}]", conn);
            }
        } catch (RepositoryException e) {
            logger.warn("Connection [{}] cannot be closed", conn, e);
        }
    }
}
