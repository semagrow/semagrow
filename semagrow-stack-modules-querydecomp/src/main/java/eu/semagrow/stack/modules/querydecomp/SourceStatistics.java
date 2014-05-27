package eu.semagrow.stack.modules.querydecomp;

import org.openrdf.model.URI;

/**
 * Created by angel on 5/27/14.
 */
public interface SourceStatistics {

    double getCostPerRecord(URI endpoint);

}
