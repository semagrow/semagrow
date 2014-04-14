package eu.semagrow.modules.sparqlutils;

import eu.semagrow.stack.modules.commons.CONSTANTS;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

/**
 *
 * @author turnguard
 */
public class SparqlUtilsTest {
    
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

    @Test
    public void httpTupleQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpTupleQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpTupleQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpTupleQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpGraphQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpGraphQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpGraphQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpGraphQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpBooleanQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpBooleanQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void httpBooleanQueryWithSuitableAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailTupleQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailTupleQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailTupleQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailTupleQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailGraphQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailGraphQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailGraphQueryWithSuitableAccept1() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailGraphQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailBooleanQueryWithEmptyAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailBooleanQueryWithUnsuitableAccept() throws RepositoryException, MalformedQueryException{       
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
    
    @Test
    public void sailBooleanQueryWithSuitableAccept() throws RepositoryException, MalformedQueryException{       
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
