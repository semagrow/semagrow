package eu.semagrow.cassandra.scraper;

import eu.semagrow.cassandra.connector.CassandraClient;
import org.openrdf.rio.RDFHandlerException;

import java.io.*;

/**
 * Created by antonis on 6/4/2016.
 */
public class CassandraScraper {

    public static void main(String [] args) throws RDFHandlerException, IOException {

        if (args.length != 7) {
            throw new IllegalArgumentException("Usage: cassandraScraper [address] [port] [keyspace] [base] [endpoint] [cassandra desc output file] [void desc output file]");
        }

        String address = args[0];
        int port = Integer.valueOf(args[1]);
        String keyspace = args[2];
        String base = args[3];
        String endpoint = args[4];
        String descPath = args[5];
        String voidPath = args[6];

        File descFile = new File(descPath);
        File voidFile = new File(voidPath);

        if (!descFile.exists()) {
            descFile.createNewFile();
        }
        if (!voidFile.exists()) {
            voidFile.createNewFile();
        }

        if (!endpoint.contains("cassandra")) {
            throw new IllegalArgumentException("Endpoint string should contain the substring \"cassandra\"");
        }

        CassandraClient client = new CassandraClient();
        client.setCredentials(address, port, keyspace);
        client.connect();

        CassandraSchemaMetatataWriter schemaWriter = new CassandraSchemaMetatataWriter();
        CassandraTriplesMetadataWriter sevodWriter = new CassandraTriplesMetadataWriter();

        schemaWriter.setClient(client);
        sevodWriter.setClient(client);

        schemaWriter.setBase(base);
        schemaWriter.setEndpoint(endpoint);
        sevodWriter.setBase(base);
        sevodWriter.setEndpoint(endpoint);

        schemaWriter.writeMetadata(new PrintStream(descFile));
        sevodWriter.writeMetadata(new PrintStream(voidFile));

        client.close();




    }



}
