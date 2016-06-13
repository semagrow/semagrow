package eu.semagrow.querylog.api;


import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.Date;
import java.util.List;

/**
 * Created by angel on 10/20/14.
 */
public interface QueryLogRecord {
    IRI getEndpoint();

    String getQuery();

    TupleExpr getExpr();

    BindingSet getBindings();

    java.util.UUID getSession();

    List<String> getBindingNames();

    void setCardinality(long card);

    long getCardinality();

    void setDuration(long start, long end);

    void setResults(IRI handle);

    Date getStartTime();

    Date getEndTime();

    long getDuration();

    IRI getResults();
}
