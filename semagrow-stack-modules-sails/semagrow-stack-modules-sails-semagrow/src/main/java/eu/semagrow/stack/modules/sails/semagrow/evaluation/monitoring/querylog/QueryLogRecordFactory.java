package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Set;

/**
 * Created by angel on 10/22/14.
 */
public interface QueryLogRecordFactory {

    QueryLogRecord createQueryLogRecord(URI endpoint,
                                        TupleExpr expr,
                                        Set<String> bindingNames);

}
