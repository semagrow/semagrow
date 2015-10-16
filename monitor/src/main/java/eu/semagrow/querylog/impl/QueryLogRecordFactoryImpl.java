package eu.semagrow.querylog.impl;

import eu.semagrow.querylog.api.QueryLogRecord;
import eu.semagrow.querylog.api.QueryLogRecordFactory;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 10/22/14.
 */
public class QueryLogRecordFactoryImpl implements QueryLogRecordFactory {

    private static QueryLogRecordFactory singleton = new QueryLogRecordFactoryImpl();


    @Override
    public QueryLogRecord createQueryLogRecord(URI endpoint,
                                               TupleQuery expr,
                                               BindingSet bindingNames)
    {
        // TODO: session is null
        return new QueryLogRecordImpl(null, endpoint, expr, bindingNames);
    }

    public static QueryLogRecordFactory getInstance() {
        return singleton;
    }

}
