package org.semagrow.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public final class SEMAGROW {

    public static final String NAMESPACE = "http://schema.semagrow.eu/";
    
    public static final class SYSTEM {
        
        public static final String NAMESPACE = SEMAGROW.NAMESPACE+"system/1.0.0/";
        
        public static final class SPARQL_SAMPLES {
            public static final String NAMESPACE = SEMAGROW.SYSTEM.NAMESPACE+"samples/";
            /* Classes */
            public static final IRI SPARQL_SAMPLE;
            /* Predicates */
            
            /*  http://schema.semagrow.eu/system/1.0.0/samples/sparqlSampleText */
            public static final IRI SPARQL_SAMPLE_TEXT;
            
            static {
                SPARQL_SAMPLE = SimpleValueFactory.getInstance().createIRI(SEMAGROW.SYSTEM.SPARQL_SAMPLES.NAMESPACE, "SparqlSample");
                SPARQL_SAMPLE_TEXT = SimpleValueFactory.getInstance().createIRI(SEMAGROW.SYSTEM.SPARQL_SAMPLES.NAMESPACE, "sparqlSampleText");
            }
        }
    }

}
