package eu.semagrow.querylog.api;

import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 10/22/14.
 */
public interface QueryLogRecordFactory {

    QueryLogRecord createQueryLogRecord(URI endpoint,
                                        TupleQuery expr,
                                        BindingSet bindingNames);

}
