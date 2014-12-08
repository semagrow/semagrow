package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog;

import java.io.OutputStream;

/**
 * Created by angel on 10/21/14.
 */
public interface QueryLogFactory {

    QueryLogWriter getQueryLogger(OutputStream out);

}
