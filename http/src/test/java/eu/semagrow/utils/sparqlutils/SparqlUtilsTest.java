package eu.semagrow.utils.sparqlutils;

import eu.semagrow.commons.CONSTANTS;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

/**
 *
 * @author turnguard
 */
public class SparqlUtilsTest extends TestCase {
    
    public SparqlUtilsTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }


    public void testhttpTupleQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"),
                                    ""), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }
    

    public void testhttpTupleQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.RDF_RDFXML), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testhttpTupleQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    } 
    

    public void testhttpTupleQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_XML), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_XML);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testhttpGraphQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    ""), 
                    CONSTANTS.MIMETYPES.RDF_TURTLE);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }
    

    public void testhttpGraphQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON), 
                    CONSTANTS.MIMETYPES.RDF_TURTLE);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testhttpGraphQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.RDF_TURTLE), 
                    CONSTANTS.MIMETYPES.RDF_TURTLE);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    } 
    

    public void testhttpGraphQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.RDF_RDFXML), 
                    CONSTANTS.MIMETYPES.RDF_RDFXML);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }      
    

    public void testhttpBooleanQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareBooleanQuery(QueryLanguage.SPARQL, "ASK { ?s ?p ?o }"), 
                                    ""), 
                    CONSTANTS.MIMETYPES.TEXT_PLAIN);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }
    

    public void testhttpBooleanQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareBooleanQuery(QueryLanguage.SPARQL, "ASK { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON), 
                    CONSTANTS.MIMETYPES.TEXT_PLAIN);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testhttpBooleanQueryWithSuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new HTTPRepository("http://localhost");
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareBooleanQuery(QueryLanguage.SPARQL, "ASK { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.TEXT_PLAIN), 
                    CONSTANTS.MIMETYPES.TEXT_PLAIN);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    } 
    

    public void testsailTupleQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"), 
                                    ""), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }
    

    public void testsailTupleQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.RDF_RDFXML), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testsailTupleQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    } 
    

    public void testsailTupleQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_XML), 
                    CONSTANTS.MIMETYPES.SPARQLRESULTS_XML);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testsailGraphQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    ""), 
                    CONSTANTS.MIMETYPES.RDF_TURTLE);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }
    

    public void testsailGraphQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON), 
                    CONSTANTS.MIMETYPES.RDF_TURTLE);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testsailGraphQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.RDF_TURTLE), 
                    CONSTANTS.MIMETYPES.RDF_TURTLE);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    } 
    

    public void testsailGraphQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.RDF_RDFXML), 
                    CONSTANTS.MIMETYPES.RDF_RDFXML);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }      
    

    public void testsailBooleanQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareBooleanQuery(QueryLanguage.SPARQL, "ASK { ?s ?p ?o }"), 
                                    ""), 
                    CONSTANTS.MIMETYPES.TEXT_PLAIN);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }
    

    public void testsailBooleanQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareBooleanQuery(QueryLanguage.SPARQL, "ASK { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON), 
                    CONSTANTS.MIMETYPES.TEXT_PLAIN);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
    

    public void testsailBooleanQueryWithSuitableAccept() throws RepositoryException, MalformedQueryException{
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            assertEquals(
                    SparqlUtils
                            .getAcceptMimeType(
                                    repCon.prepareBooleanQuery(QueryLanguage.SPARQL, "ASK { ?s ?p ?o }"), 
                                    CONSTANTS.MIMETYPES.TEXT_PLAIN), 
                    CONSTANTS.MIMETYPES.TEXT_PLAIN);
        } finally {
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }     
}
