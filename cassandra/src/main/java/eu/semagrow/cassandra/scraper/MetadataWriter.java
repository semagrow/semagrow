package eu.semagrow.cassandra.scraper;

import eu.semagrow.cassandra.connector.CassandraClient;
import org.openrdf.rio.RDFHandlerException;

import java.io.PrintStream;

/**
 * Created by antonis on 15/4/2016.
 */
public interface MetadataWriter {

    void setClient(CassandraClient client);

    void writeMetadata(PrintStream stream);
}
