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

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

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
        ValueFactory vf = SimpleValueFactory.getInstance();
        try {
            rep.initialize();
            repCon = rep.getConnection();
            repCon.begin();
            repCon.add(vf.createIRI("urn:x"), vf.createIRI("urn:p"), vf.createIRI("urn:p-uri"));
            repCon.add(vf.createIRI("urn:x"), vf.createIRI("urn:p"), vf.createLiteral("p1-lit"));
            repCon.add(vf.createIRI("urn:x"), vf.createIRI("urn:p"), vf.createLiteral("p1-lit-en","en"));
            repCon.add(vf.createIRI("urn:x"), vf.createIRI("urn:p"), vf.createLiteral("p1-lit-typed",XMLSchema.STRING));
            repCon.add(vf.createIRI("urn:x"), vf.createIRI("urn:p"), vf.createLiteral("true",XMLSchema.BOOLEAN));
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
