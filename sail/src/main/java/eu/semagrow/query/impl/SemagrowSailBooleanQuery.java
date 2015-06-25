package eu.semagrow.query.impl;

import eu.semagrow.query.SemagrowBooleanQuery;
import eu.semagrow.sail.SemagrowSailConnection;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailException;

/**
 * Created by angel on 6/13/14.
 */
public class SemagrowSailBooleanQuery extends SemagrowSailQuery
        implements SemagrowBooleanQuery {


    public SemagrowSailBooleanQuery(ParsedBooleanQuery tupleQuery,
                                    SailRepositoryConnection sailConnection) {
        super(tupleQuery, sailConnection);
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
            SemagrowSailConnection sailCon = (SemagrowSailConnection) getConnection().getSailConnection();

            CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter;
            bindingsIter = sailCon.evaluate(tupleExpr, dataset, getBindings(), getIncludeInferred(), false,
                    getIncludedSources(), getExcludedSources());

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
}
