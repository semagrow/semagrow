package eu.semagrow.stack.modules.sails.semagrow.query;

import eu.semagrow.stack.modules.api.decomposer.QueryDecompositionException;
import eu.semagrow.stack.modules.api.query.SemagrowTupleQuery;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailRepositoryConnection;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailConnection;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.SailException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 6/9/14.
 */
public class SemagrowSailTupleQuery extends SailTupleQuery implements SemagrowTupleQuery {

    private boolean includeProvenanceData = false;

    private Set<URI> excludedSources;
    private Set<URI> includeOnlySources;


    public SemagrowSailTupleQuery(ParsedTupleQuery query, SemagrowSailRepositoryConnection connection)
    {
        super(query,connection);
        excludedSources = new HashSet<URI>();
        includeOnlySources = new HashSet<URI>();
    }

    public TupleQueryResult evaluate() throws QueryEvaluationException {

        TupleExpr tupleExpr = getParsedQuery().getTupleExpr();

        try {
            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;

            SemagrowSailConnection sailCon = (SemagrowSailConnection) getConnection().getSailConnection();

            bindingsIter = sailCon.evaluate(tupleExpr, getActiveDataset(), getBindings(),
                    getIncludeInferred(), getIncludeProvenanceData());

            bindingsIter = enforceMaxQueryTime(bindingsIter);

            return new TupleQueryResultImpl(new ArrayList<String>(tupleExpr.getBindingNames()), bindingsIter);
        }
        catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

    public void setIncludeProvenanceData(boolean includeProvenance) {
        includeProvenanceData = includeProvenance;
    }

    public boolean getIncludeProvenanceData() { return includeProvenanceData; }

    @Override
    public SemagrowSailRepositoryConnection getConnection() {
        return (SemagrowSailRepositoryConnection) super.getConnection();
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
