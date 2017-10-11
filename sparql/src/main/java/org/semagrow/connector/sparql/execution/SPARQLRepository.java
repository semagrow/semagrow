package org.semagrow.connector.sparql.execution;

import org.semagrow.model.SemagrowValueFactory;
import org.eclipse.rdf4j.http.client.SparqlSession;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Created by angel on 8/6/2016.
 */
public class SPARQLRepository extends org.eclipse.rdf4j.repository.sparql.SPARQLRepository {

    private boolean quadMode = false;

    public SPARQLRepository(String endpointUrl) {
        super(endpointUrl);
        setValueFactory(SemagrowValueFactory.getInstance());
    }

    private ValueFactory valueFactory;

    public void setValueFactory(ValueFactory vf) { valueFactory = vf; }

    @Override
    public SPARQLConnection getConnection() throws RepositoryException {

        if (!isInitialized()) {
            throw new RepositoryException("SPARQLRepository not initialized.");
        }
        return new SPARQLConnection(this, createHTTPClient(), quadMode);
    }

    @Override
    public ValueFactory getValueFactory() { return valueFactory; }

    @Override
    protected SparqlSession createHTTPClient() {
        SparqlSession session = super.createHTTPClient();
        session.setConnectionTimeout(10000);
        session.setValueFactory(getValueFactory());
        return session;
    }

    /**
     * Activate quad mode for this {@link SPARQLRepository}, i.e. for
     * retrieval of statements also retrieve the graph.<p>
     *
     * Note: the setting is only applied in newly created {@link SPARQLConnection}s
     * as the setting is an immutable configuration of a connection instance.
     *
     * @param flag flag to enable or disable the quad mode
     * @see SPARQLConnection#getStatements(org.openrdf.model.Resource, org.openrdf.model.URI, org.openrdf.model.Value, boolean, org.openrdf.model.Resource...)
     */
    @Override
    public void enableQuadMode(boolean flag) {
        this.quadMode = flag;
    }
}
