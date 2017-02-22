package org.semagrow.query.impl;

import org.semagrow.query.SemagrowBooleanQuery;
import org.semagrow.sail.SemagrowSailConnection;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;

/**
 * Created by angel on 6/13/14.
 */
public class SemagrowSailBooleanQuery extends SemagrowSailQuery
        implements SemagrowBooleanQuery {


    public SemagrowSailBooleanQuery(ParsedBooleanQuery tupleQuery,
                                    String queryString,
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
