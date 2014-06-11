package eu.semagrow.stack.modules.sails.semagrow.query;

import eu.semagrow.stack.modules.api.query.SemagrowTupleQuery;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailRepositoryConnection;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailConnection;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.SailException;

import java.util.ArrayList;

/**
 * Created by angel on 6/9/14.
 */
public class SemagrowSailTupleQuery extends SailTupleQuery implements SemagrowTupleQuery {

    private boolean includeProvenanceData = false;

    public SemagrowSailTupleQuery(ParsedTupleQuery query, SemagrowSailRepositoryConnection connection)
    {
        super(query,connection);
    }

    @Override
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
}
