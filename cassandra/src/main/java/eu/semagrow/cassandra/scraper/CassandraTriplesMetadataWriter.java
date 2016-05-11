package eu.semagrow.cassandra.scraper;

import com.datastax.driver.core.*;
import eu.semagrow.cassandra.connector.CassandraClient;
import eu.semagrow.cassandra.mapping.RdfMapper;
import eu.semagrow.commons.vocabulary.SEVOD;
import eu.semagrow.commons.vocabulary.VOID;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * Created by antonis on 29/3/2016.
 */
public class CassandraTriplesMetadataWriter implements MetadataWriter {

    private final Logger logger = LoggerFactory.getLogger(CassandraTriplesMetadataWriter.class);

    private CassandraClient client;
    private String base;
    private URI endpoint;

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    @Override
    public void setClient(CassandraClient client) {
        this.client = client;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = vf.createURI(endpoint);
    }

    @Override
    public void writeMetadata(PrintStream stream){

        try {
            Resource root = vf.createBNode();

            RDFWriter writer  = new CompactBNodeTurtleWriter(stream);

            writer.startRDF();

            writer.handleNamespace("void", VOID.NAMESPACE);
            writer.handleNamespace("svd", SEVOD.NAMESPACE);
            writer.handleNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");

            writer.handleStatement(vf.createStatement(root, RDF.TYPE, VOID.DATASET));

            long triples = 0;

            for (TableMetadata tableMetadata: client.getTables()) {
                String tableName = tableMetadata.getName();
                for (ColumnMetadata columnMetadata: tableMetadata.getColumns()) {
                    if (columnMetadata.getParent().getPartitionKey().contains(columnMetadata)) {
                        writePrimaryColumnMetadata(writer, root, base, columnMetadata.getName(), tableName);
                    } else {
                        writeRegularColumnMetadata(writer, root, base, columnMetadata.getName(), tableName);
                    }
                }
                triples += client.executeCount("SELECT COUNT(*) FROM " + tableName + ";");
            }

            writer.handleStatement(vf.createStatement(root, VOID.SPARQLENDPOINT, endpoint));
            writer.handleStatement(vf.createStatement(root, VOID.URISPACE, vf.createLiteral(base)));
            writer.handleStatement(vf.createStatement(root, VOID.TRIPLES, vf.createLiteral(triples)));

            writer.endRDF();

        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writePrimaryColumnMetadata(RDFWriter writer, Resource root, String base, String columnName, String tableName) throws RDFHandlerException {

        long triples = client.executeCount("SELECT COUNT(*) FROM " + tableName + ";");

        Resource partition = vf.createBNode();
        URI property = RdfMapper.getUriFromColumn(base, tableName, columnName);
        writer.handleStatement(vf.createStatement(root, VOID.PROPERTYPARTITION, partition));
        writer.handleStatement(vf.createStatement(partition, VOID.PROPERTY, property));
        writer.handleStatement(vf.createStatement(partition, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(partition, VOID.DISTINCTSUBJECTS, vf.createLiteral(triples)));
    }

    private void writeRegularColumnMetadata(RDFWriter writer, Resource root, String base, String columnName, String tableName) throws RDFHandlerException {

        long triples = client.executeCount("SELECT COUNT(" + columnName + ") FROM " + tableName + ";");

        Resource partition = vf.createBNode();
        URI property = RdfMapper.getUriFromColumn(base, tableName, columnName);
        writer.handleStatement(vf.createStatement(root, VOID.PROPERTYPARTITION, partition));
        writer.handleStatement(vf.createStatement(partition, VOID.PROPERTY, property));
        writer.handleStatement(vf.createStatement(partition, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(partition, VOID.DISTINCTSUBJECTS, vf.createLiteral(triples)));
    }
}
