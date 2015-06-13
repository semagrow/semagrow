package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.sails.config.VOIDInferencerConfig;
import eu.semagrow.stack.modules.sails.semagrow.config.SemagrowSailConfig;
import eu.semagrow.stack.modules.sails.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.stack.modules.api.query.SemagrowTupleQuery;
import eu.semagrow.stack.modules.vocabulary.VOID;
import info.aduna.iteration.Iterations;
import junit.framework.TestCase;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.repository.config.RepositoryRegistry;
import org.openrdf.repository.sail.config.SailRepositoryConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.config.SailImplConfig;
import org.openrdf.sail.inferencer.fc.config.ForwardChainingRDFSInferencerConfig;
import org.openrdf.sail.memory.config.MemoryStoreConfig;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class SemagrowConnectionTest extends TestCase {

    private String PREFIX = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX void: <http://rdfs.org/ns/void#>\n";

    public void testEvaluateInternal() throws Exception {

        String q = "SELECT ?drug ?title WHERE { \n" +
                "  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/micronutrient> .\n" +
                "  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> ?id .\n" +
                "  ?keggDrug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Drug> .\n" +
                "  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?id .\n" +
                "  ?keggDrug <http://purl.org/dc/elements/1.1/title> ?title .\n" +
                "}";

        List<String> mf = new ArrayList<String>();
        mf.add("/home/antonis/Documents/lifeScience.svd.n3");

        SailImplConfig config = new SemagrowSailConfig();

        SemagrowRepositoryConfig repoConfig = new SemagrowRepositoryConfig();
        repoConfig.getSemagrowSailConfig().setInitialFiles(mf);
        repoConfig.getSemagrowSailConfig().setExecutorBatchSize(20);

        SemagrowSailRepository repo = (SemagrowSailRepository) RepositoryRegistry.getInstance().get(repoConfig.getType()).getRepository(repoConfig);
        repo.initialize();
        SemagrowSailRepositoryConnection conn = repo.getConnection();
        SemagrowTupleQuery query =  conn.prepareTupleQuery(QueryLanguage.SPARQL, q);
        query.setIncludeInferred(true);
        query.setIncludeProvenanceData(true);

        final CountDownLatch latch = new CountDownLatch(1);


        query.evaluate(new TupleQueryResultHandler() {
            @Override
            public void handleBoolean(boolean b) throws
                    QueryResultHandlerException {

            }

            @Override
            public void handleLinks(List<String> list) throws
                    QueryResultHandlerException {

            }

            @Override
            public void startQueryResult(List<String> list) throws
                    TupleQueryResultHandlerException {

            }

            @Override
            public void endQueryResult() throws
                    TupleQueryResultHandlerException {
                latch.countDown();
            }

            @Override
            public void handleSolution(BindingSet bindingSet) throws
                    TupleQueryResultHandlerException {
                System.out.println(bindingSet);
            }
        });

        latch.await();
    }

    public void testEvaluateInternal1() throws Exception {

        String q1 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX void: <http://rdfs.org/ns/void#>\n" +
                "SELECT *  { ?s <http://localhost/my> ?z. " +
                "?z <http://rdf.iit.demokritos.gr/2014/my#pred2> ?y . }" ;


        String q = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX void: <http://rdfs.org/ns/void#>\n" +
                "SELECT *  { { <htt://localhost/sub> <http://localhost/my> ?z. " +
                "?z <http://rdf.iit.demokritos.gr/2014/my#pred2> \"R\" . } UNION " +
                "{ <htt://localhost/sub> <http://localhost/my> ?z.\n" +
                "?z <http://rdf.iit.demokritos.gr/2014/my#pred2> \"R\" . } } LIMIT 10" ;

        SailImplConfig config = new SemagrowSailConfig();

        SemagrowRepositoryConfig repoConfig = new SemagrowRepositoryConfig();
        SemagrowSailRepository repo = (SemagrowSailRepository) RepositoryRegistry.getInstance().get(repoConfig.getType()).getRepository(repoConfig);
        repo.initialize();
        SemagrowSailRepositoryConnection conn = repo.getConnection();
        SemagrowTupleQuery query =  conn.prepareTupleQuery(QueryLanguage.SPARQL, q1);
        query.setIncludeInferred(true);
        query.setIncludeProvenanceData(true);

        TupleQueryResult result = query.evaluate();
        System.out.println(Iterations.toString(result, "\n"));
        Iterations.closeCloseable(result);
    }


    public void testEvaluateInternal2() throws Exception {
    	
    	String q3 = "SELECT * WHERE {"
    			+ " ?document <http://purl.org/ontology/bibo/abstract> ?abstract . "
    			+ " ?document <http://purl.org/dc/terms/title> ?title . "
    			+ "}" ;
    	
    	String q2 = "SELECT * WHERE {"
    			+ " ?document <http://purl.org/ontology/bibo/abstract> ?abstract . "
    			+ " ?document <http://purl.org/dc/terms/title> ?title . "
    			+ " ?document <http://purl.org/dc/terms/creator> ?creator . "
    			+ "}" ;


        String q1 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX void: <http://rdfs.org/ns/void#>\n" +
                "SELECT *  { ?s ?p ?z. }" ;


        String q = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX void: <http://rdfs.org/ns/void#>\n" +
                "SELECT *  { <htt://localhost/sub> <http://localhost/my> ?z. " +
                "?z <http://rdf.iit.demokritos.gr/2014/my#pred2> \"R\" . } " ;

        SailImplConfig config = new SemagrowSailConfig();

        SemagrowRepositoryConfig repoConfig = new SemagrowRepositoryConfig();
        SemagrowSailRepository repo = (SemagrowSailRepository) RepositoryRegistry.getInstance().get(repoConfig.getType()).getRepository(repoConfig);
        repo.initialize();
        SemagrowSailRepositoryConnection conn = repo.getConnection();
        SemagrowTupleQuery query =  conn.prepareTupleQuery(QueryLanguage.SPARQL, q3);
        query.setIncludeInferred(true);
        query.setIncludeProvenanceData(true);

        TupleQueryResult result = query.evaluate();
        System.out.println(Iterations.toString(result, "\n"));
        Iterations.closeCloseable(result);
    }

    public void testCrossProduct() throws Exception {

        String q = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX void: <http://rdfs.org/ns/void#>\n" +
                "SELECT *  { <htt://localhost/sub> <http://localhost/my> ?z. " +
                "?z2 <http://rdf.iit.demokritos.gr/2014/my#pred2> ?w . } " ;

        SailImplConfig config = new SemagrowSailConfig();

        SemagrowRepositoryConfig repoConfig = new SemagrowRepositoryConfig();
        SemagrowSailRepository repo = (SemagrowSailRepository) RepositoryRegistry.getInstance().get(repoConfig.getType()).getRepository(repoConfig);
        repo.initialize();
        SemagrowSailRepositoryConnection conn = repo.getConnection();
        SemagrowTupleQuery query =  conn.prepareTupleQuery(QueryLanguage.SPARQL, q);
        query.setIncludeInferred(true);
        query.setIncludeProvenanceData(true);

        TupleQueryResult result = query.evaluate();
        System.out.println(Iterations.toString(result, "\n"));
        Iterations.closeCloseable(result);
    }

    public void testVOID() throws Exception {
        String q = PREFIX +
                "SELECT * FROM <http://www.semagrow.eu/metadata> { " +
                "?s <" + VOID.PROPERTY +  "> ?o. }";
        //SailImplConfig config = new SemagrowConfig();

        //SailRepositoryConfig repoConfig = new SailRepositoryConfig(config);
        RepositoryImplConfig repoConfig = new SemagrowRepositoryConfig();
        Repository repo = RepositoryRegistry.getInstance().get(repoConfig.getType()).getRepository(repoConfig);
        //Repository repo = RepositoryRegistry.getInstance().get()
        repo.initialize();
        RepositoryConnection conn = repo.getConnection();
        TupleQuery query =  conn.prepareTupleQuery(QueryLanguage.SPARQL, q);
        query.setIncludeInferred(true);
        query.setBinding("o", ValueFactoryImpl.getInstance().createURI("http://localhost/my"));
        TupleQueryResult result = query.evaluate();

        int i = 0;
        while(result.hasNext()) {
            System.out.println(result.next().toString());
            i++;
        }
        System.out.println(i);
    }

    public void testVOIDInference() throws Exception {
        String q = PREFIX +
                "SELECT DISTINCT ?endpoint { " +
                "?s void:sparqlEndpoint ?endpoint . " +
                " }";
        SailImplConfig config = new
                VOIDInferencerConfig(new ForwardChainingRDFSInferencerConfig(
                new MemoryStoreConfig(false)));

        SailRepositoryConfig repoConfig = new SailRepositoryConfig(config);
        Repository repo = RepositoryRegistry.getInstance().get(repoConfig.getType()).getRepository(repoConfig);
        repo.initialize();
        loadRepo(repo);
        RepositoryConnection conn = repo.getConnection();
        TupleQuery query =  conn.prepareTupleQuery(QueryLanguage.SPARQL, q);
        query.setIncludeInferred(true);
        TupleQueryResult result = query.evaluate();

        int i = 0;
        while(result.hasNext()) {
            System.out.println(result.next().toString());
            i++;
        }
        System.out.println(i);
    }

    private void loadRepo(Repository repo) throws Exception {
        RepositoryConnection conn = repo.getConnection();
        conn.add(new File("/tmp/metadata.ttl"), "file:///tmp/metadata.ttl", RDFFormat.TURTLE);
        conn.commit();
        conn.close();
    }
}