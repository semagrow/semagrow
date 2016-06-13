package eu.semagrow.querylog.api;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;

/**
 * Created by angel on 10/22/14.
 */
public interface QueryLogRecordFactory {

    QueryLogRecord createQueryLogRecord(IRI endpoint,
                                        String expr,
                                        BindingSet bindingNames);

}
