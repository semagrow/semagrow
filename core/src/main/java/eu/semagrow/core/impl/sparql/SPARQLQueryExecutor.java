package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import eu.semagrow.querylog.api.QueryLogHandler;

/**
 * Created by angel on 6/4/2016.
 */
public class SPARQLQueryExecutor extends QueryExecutorImpl {

    public SPARQLQueryExecutor(QueryLogHandler qfrHandler, MaterializationManager mat) {
        super(qfrHandler, mat);
    }

}
