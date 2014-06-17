package eu.semagrow.stack.modules.sails.semagrow.query;

import eu.semagrow.stack.modules.api.decomposer.QueryDecompositionException;
import eu.semagrow.stack.modules.api.query.SemagrowBooleanQuery;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailConnection;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailRepositoryConnection;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.repository.sail.SailBooleanQuery;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 6/13/14.
 */
public class SemagrowSailBooleanQuery extends SailBooleanQuery
        implements SemagrowBooleanQuery {


    private Set<URI> excludedSources;
    private Set<URI> includeOnlySources;


    public SemagrowSailBooleanQuery(ParsedBooleanQuery tupleQuery,
                                    SemagrowSailRepositoryConnection sailConnection) {
        super(tupleQuery, sailConnection);

        excludedSources = new HashSet<URI>();
        includeOnlySources = new HashSet<URI>();
    }

    @Override
    public ParsedBooleanQuery getParsedQuery() {
        return (ParsedBooleanQuery)super.getParsedQuery();
    }

    public boolean evaluate() throws QueryEvaluationException {

        ParsedBooleanQuery parsedBooleanQuery = getParsedQuery();
        TupleExpr tupleExpr = parsedBooleanQuery.getTupleExpr();
        Dataset dataset = getDataset();
        if (dataset == null) {
            // No external dataset specified, use query's own dataset (if any)
            dataset = parsedBooleanQuery.getDataset();
        }

        try {
            SailConnection sailCon = getConnection().getSailConnection();

            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;
            bindingsIter = sailCon.evaluate(tupleExpr, dataset, getBindings(), getIncludeInferred());

            bindingsIter = enforceMaxQueryTime(bindingsIter);

            try {
                return bindingsIter.hasNext();
            }
            finally {
                bindingsIter.close();
            }
        }
        catch (SailException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }
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
