package eu.semagrow.query;

import org.openrdf.query.TupleQuery;

/**
 * Created by angel on 6/8/14.
 */
public interface SemagrowTupleQuery extends SemagrowQuery, TupleQuery {

    void setIncludeProvenanceData(boolean includeProvenance);

    boolean getIncludeProvenanceData();

}
