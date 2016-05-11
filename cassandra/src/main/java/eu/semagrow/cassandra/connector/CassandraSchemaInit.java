package eu.semagrow.cassandra.connector;

import eu.semagrow.cassandra.vocab.CDV;
import eu.semagrow.commons.utils.FileUtils;
import eu.semagrow.commons.vocabulary.VOID;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by antonis on 12/4/2016.
 */
public class CassandraSchemaInit {

    //private static final String path = "/home/antonis/Documents/cassandra/cdfheaderdesc.n3";

    private static final String queryPrefix = "" +
            "PREFIX cdv:  <"  + CDV.NAMESPACE  + "> \n" +
            "PREFIX void: <" + VOID.NAMESPACE  + "> \n ";

    private Map<String, CassandraSchema> schemaMap = new HashMap<>();

    private CassandraSchemaInit() {}

    private static CassandraSchemaInit instance = null;

    public static CassandraSchemaInit getInstance() {
        if (instance == null) {
            File file = null;
            try {
                file = FileUtils.getFile("cassandra.ttl");
            } catch (IOException e) {
                e.printStackTrace();
            }
            instance = new CassandraSchemaInit();
            instance.initialize(file);
        }
        return instance;
    }

    public CassandraSchema getCassandraSchema(URI endpoint) {
        return schemaMap.get(endpoint.stringValue());
    }

    private void initialize(File file) {

        try {
            Repository repo = new SailRepository(new MemoryStore());
            repo.initialize();

            RDFFormat fileFormat = RDFFormat.forFileName(file.getAbsolutePath(), RDFFormat.N3);

            RepositoryConnection conn = repo.getConnection();
            conn.add(file, file.toURI().toString(), fileFormat);

            initializeMap(conn);
            processCredentials(conn);
            processTables(conn);
            processColumns(conn);
            processIndices(conn);

            conn.close();
            repo.shutDown();

        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (RDFParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initializeMap(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = new CassandraSchema();
                schemaMap.put(bs.getValue("endpoint").stringValue(), cs);
            }
        } finally {
            results.close();
        }
    }

    private void processCredentials(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?address ?port ?keynote ?base WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?d cdv:address ?address . \n" +
                "   ?d cdv:port ?port . \n" +
                "   ?d cdv:keynote ?keynote . \n" +
                "   ?d cdv:base ?base . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                cs.setBase(bs.getValue("base").stringValue());
                cs.setCredentials(
                        bs.getValue("address").stringValue(),
                        Integer.valueOf(bs.getValue("port").stringValue()),
                        bs.getValue("keynote").stringValue()
                );
            }
        } finally {
            results.close();
        }
    }

    private void processTables(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?name WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?d cdv:base ?base . \n" +
                "   ?d cdv:tables ?table . \n" +
                "   ?table cdv:name ?name . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                cs.addTable(bs.getValue("name").stringValue());
            }
        } finally {
            results.close();
        }
    }

    private void processColumns(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?columnname ?tablename ?type ?position WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?table cdv:name ?tablename . \n" +
                "   ?table cdv:tableSchema ?schema . \n" +
                "   ?schema cdv:columns ?column . \n" +
                "   ?column cdv:columnType ?type . \n" +
                "   ?column cdv:name ?columnname . \n" +
                "   OPTIONAL { ?column cdv:clusteringPosition ?position . } \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                URI type = (URI) bs.getValue("type");
                String column = bs.getValue("columnname").stringValue();
                String table = bs.getValue("tablename").stringValue();

                if (type.equals(CDV.PARTITION)) {
                    cs.addPartitionColumn(table, column);
                }
                if (type.equals(CDV.CLUSTERING)) {
                    int position = Integer.valueOf(bs.getValue("position").stringValue());
                    cs.addClusteringColumn(table, column, position);
                }
                if (type.equals(CDV.REGULAR)) {
                    cs.addRegularColumn(table, column);
                }
            }
        } finally {
            results.close();
        }
    }

    private void processIndices(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?columnname ?tablename WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?table cdv:name ?tablename . \n" +
                "   ?table cdv:tableSchema ?schema . \n" +
                "   ?schema cdv:secondaryIndex ?columnname . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                String column = bs.getValue("columnname").stringValue();
                String table = bs.getValue("tablename").stringValue();

                cs.addIndex(table,column);
            }
        } finally {
            results.close();
        }
    }
}
