package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.impl;

import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.file.MaterializationHandle;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring.querylog.QueryLogRecord;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
* Created by angel on 10/20/14.
*/
public class QueryLogRecordImpl implements QueryLogRecord {

    private QueryEvaluationSession session;

    private TupleExpr query;

    private URI endpoint;

    private List<String> bindingNames;

    private long cardinality;

    private Date startTime;

    private Date endTime;

    private long duration;

    private MaterializationHandle results;

    public QueryLogRecordImpl(QueryEvaluationSession session, URI endpoint, TupleExpr query) {
        this.session = session;
        this.endpoint = endpoint;
        this.query = query;
        this.bindingNames = new LinkedList<String>();
    }

    public QueryLogRecordImpl(QueryEvaluationSession session, URI endpoint, TupleExpr query, Collection<String> bindingNames) {
        this.session = session;
        this.endpoint = endpoint;
        this.query = query;
        this.bindingNames = new LinkedList<String>(bindingNames);
    }

    @Override
    public URI getEndpoint() { return endpoint; }

    @Override
    public TupleExpr getQuery() { return query; }

    @Override
    public QueryEvaluationSession getSession() { return session; }

    @Override
    public List<String> getBindingNames() { return bindingNames; }

    @Override
    public void setCardinality(long card) { cardinality = card; }

    @Override
    public long getCardinality() { return cardinality; }

    @Override
    public void setDuration(long start, long end) {
        startTime = new Date(start);
        endTime = new Date(end);
        duration = end - start;
    }

    @Override
    public void setResults(MaterializationHandle handle) { results = handle; }

    @Override
    public Date getStartTime() { return startTime; }

    @Override
    public Date getEndTime() { return endTime; }

    @Override
    public long getDuration() { return duration; }

    @Override
    public MaterializationHandle getResults() { return results; }
}
