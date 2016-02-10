/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.semagrow.utils.queryresultio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

/**
 *
 * @author turnguard
 */
public class HTMLTableWriterTest {
    
    public HTMLTableWriterTest() {
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
    public void sailGraphQueryWithSuitableAccept2() throws RepositoryException, MalformedQueryException, QueryEvaluationException, TupleQueryResultHandlerException, IOException{       
        Repository rep = new SailRepository(new MemoryStore());
        RepositoryConnection repCon = null;
        ByteArrayOutputStream baos = null;
        try {
            rep.initialize();
            repCon = rep.getConnection();
            repCon.begin();
            repCon.add(new URIImpl("urn:x"), new URIImpl("urn:p"), new URIImpl("urn:p-uri"));
            repCon.add(new URIImpl("urn:x"), new URIImpl("urn:p"), new LiteralImpl("p1-lit"));
            repCon.add(new URIImpl("urn:x"), new URIImpl("urn:p"), new LiteralImpl("p1-lit-en","en"));
            repCon.add(new URIImpl("urn:x"), new URIImpl("urn:p"), new LiteralImpl("p1-lit-typed",XMLSchema.STRING));
            repCon.add(new URIImpl("urn:x"), new URIImpl("urn:p"), new LiteralImpl("true",XMLSchema.BOOLEAN));
            repCon.commit();
            baos = new ByteArrayOutputStream();
            repCon.prepareTupleQuery(
                    QueryLanguage.SPARQL,
                    "SELECT * WHERE { ?s ?p ?o }")
                    .evaluate(new HTMLTableWriter(baos));
            assertEquals(378,baos.size());
        } finally {
            if(baos!=null){ baos.close(); }
            if(repCon!=null){repCon.close();}
            rep.shutDown();
        }       
    }  
}
