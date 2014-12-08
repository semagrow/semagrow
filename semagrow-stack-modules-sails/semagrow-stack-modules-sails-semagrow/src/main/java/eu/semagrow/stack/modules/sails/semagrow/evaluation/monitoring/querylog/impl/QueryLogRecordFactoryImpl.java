package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.impl;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogRecord;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogRecordFactory;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Set;

/**
 * Created by angel on 10/22/14.
 */
public class QueryLogRecordFactoryImpl implements QueryLogRecordFactory {

    private static QueryLogRecordFactory singleton = new QueryLogRecordFactoryImpl();


    @Override
    public QueryLogRecord createQueryLogRecord(URI endpoint,
                                               TupleExpr expr,
                                               Set<String> bindingNames)
    {
        // TODO: session is null
        return new QueryLogRecordImpl(null, endpoint, expr, bindingNames);
    }

    public static QueryLogRecordFactory getInstance() {
        return singleton;
    }

}
