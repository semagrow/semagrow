package eu.semagrow.core.statistics;

import org.eclipse.rdf4j.model.IRI;

/**
 * Created by angel on 5/27/14.
 */
public interface SourceStatistics {

    double getCostPerRecord(IRI endpoint);

}
