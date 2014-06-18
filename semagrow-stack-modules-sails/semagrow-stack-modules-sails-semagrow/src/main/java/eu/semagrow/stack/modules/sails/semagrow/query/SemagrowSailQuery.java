package eu.semagrow.stack.modules.sails.semagrow.query;

import eu.semagrow.stack.modules.api.decomposer.QueryDecompositionException;
import eu.semagrow.stack.modules.api.query.SemagrowQuery;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailConnection;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailRepositoryConnection;
import org.openrdf.model.URI;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 6/17/14.
 */
public class SemagrowSailQuery extends SailQuery implements SemagrowQuery {

    private Set<URI> excludedSources;
    private Set<URI> includeOnlySources;

    protected SemagrowSailQuery(ParsedQuery parsedQuery, SailRepositoryConnection con) {
        super(parsedQuery, con);
        excludedSources = new HashSet<URI>();
        includeOnlySources = new HashSet<URI>();
    }

    public TupleExpr getDecomposedQuery() throws QueryDecompositionException {

        SemagrowSailConnection conn = (SemagrowSailConnection) getConnection().getSailConnection();
        TupleExpr initialExpr = getParsedQuery().getTupleExpr();
        TupleExpr expr = initialExpr.clone();
        Dataset dataset = getDataset();

        if (dataset == null) {
            // No external dataset specified, use query's own dataset (if any)
            dataset = getParsedQuery().getDataset();
        }
        return conn.decompose(expr, dataset, getBindings());
    }

    public void addExcludedSource(URI source) { excludedSources.add(source); }

    public void addIncludedSource(URI source) { includeOnlySources.add(source); }

    public Collection<URI> getExcludedSources() { return excludedSources; }

    public Collection<URI> getIncludedSources() { return includeOnlySources; }
}
