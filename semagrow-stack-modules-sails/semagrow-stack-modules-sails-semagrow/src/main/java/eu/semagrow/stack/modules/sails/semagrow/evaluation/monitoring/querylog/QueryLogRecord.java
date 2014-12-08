package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog;

import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.file.MaterializationHandle;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Date;
import java.util.List;

/**
 * Created by angel on 10/20/14.
 */
public interface QueryLogRecord {
    URI getEndpoint();

    TupleExpr getQuery();

    QueryEvaluationSession getSession();

    List<String> getBindingNames();

    void setCardinality(long card);

    long getCardinality();

    void setDuration(long start, long end);

    void setResults(MaterializationHandle handle);

    Date getStartTime();

    Date getEndTime();

    long getDuration();

    MaterializationHandle getResults();
}
