package org.semagrow.querylog.impl;

import org.semagrow.querylog.api.QueryLogRecord;
import org.semagrow.querylog.api.QueryLogRecordFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;

/**
 * Created by angel on 10/22/14.
 */
public class QueryLogRecordFactoryImpl implements QueryLogRecordFactory {

    private static QueryLogRecordFactory singleton = new QueryLogRecordFactoryImpl();


    @Override
    public QueryLogRecord createQueryLogRecord(IRI endpoint,
                                               String expr,
                                               BindingSet bindingNames)
    {
        // TODO: session is null
        return new QueryLogRecordImpl(null, endpoint, expr, bindingNames);
    }

    public static QueryLogRecordFactory getInstance() {
        return singleton;
    }

}
