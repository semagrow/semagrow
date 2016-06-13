package eu.semagrow.querylog.impl;

import eu.semagrow.querylog.api.QueryLogRecord;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.*;

/**
* Created by angel on 10/20/14.
*/
public class QueryLogRecordImpl implements QueryLogRecord {

    //private QueryEvaluationSession session;
    private UUID session;

    private String query;

    private TupleExpr expr;

    private IRI endpoint;

    private List<String> bindingNames;

    private long cardinality;

    private Date startTime;

    private Date endTime;

    private long duration;

    private IRI results;

    private BindingSet bindings;

    public QueryLogRecordImpl(UUID session, IRI endpoint, String query, BindingSet bindings) {
        this.session = session;
        this.endpoint = endpoint;
        this.query = query;
        this.expr = null;
        this.bindings = bindings;
        this.bindingNames = new LinkedList<String>();
    }

    public QueryLogRecordImpl(UUID session, IRI endpoint, TupleExpr query, BindingSet bindings) {
        this.session = session;
        this.endpoint = endpoint;
        this.expr = query;
        this.query = query.toString();
        this.bindings = bindings;
        this.bindingNames = new LinkedList<String>();
    }

   /* public QueryLogRecordImpl(QueryEvaluationSession session, URI endpoint, TupleExpr query) {
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
    }*/

    public QueryLogRecordImpl(UUID session, IRI endpoint, String query, BindingSet bindings, Collection<String> bindingNames) {
        this.session = session;
        this.endpoint = endpoint;
        this.query = query;
        this.expr = null;
        this.bindings = bindings;
        this.bindingNames = new LinkedList<String>(bindingNames);
    }

    public QueryLogRecordImpl(UUID session, IRI endpoint, TupleExpr query, BindingSet bindings, Collection<String> bindingNames) {
        this.session = session;
        this.endpoint = endpoint;
        this.expr = query;
        this.query = expr.toString();
        this.bindings = bindings;
        this.bindingNames = new LinkedList<String>(bindingNames);
    }


    @Override
    public IRI getEndpoint() { return endpoint; }

    @Override
    public String getQuery() { return query; }

    @Override
    public TupleExpr getExpr() { return expr; }

    /*
    @Override
    public QueryEvaluationSession getSession() { return session; }
    */

    @Override
    public UUID getSession() { return session; }

    @Override
    public List<String> getBindingNames() { return bindingNames; }

    @Override
    public BindingSet getBindings() {  return bindings;  }

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
    public void setResults(IRI handle) { results = handle; }

    @Override
    public Date getStartTime() { return startTime; }

    @Override
    public Date getEndTime() { return endTime; }

    @Override
    public long getDuration() { return duration; }

    @Override
    public IRI getResults() { return results; }
}
