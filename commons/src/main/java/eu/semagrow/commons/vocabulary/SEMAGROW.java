package eu.semagrow.commons.vocabulary;

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
    
    public static final class SAILS {
        public static final String NAMESPACE = SEMAGROW.NAMESPACE+"sails/1.0.0/";        
        
        public static final class POWDER {
            public static final String NAMESPACE = SEMAGROW.SAILS.NAMESPACE+"powder/";
            
            /* CLASSES */
            /**
             * SailType http://schema.semagrow.eu/sails/1.0.0/powder/PowderSail
             */
            public static final IRI POWDER_SAIL;
            
            /* PREDICATES */
            public static final IRI POSTGRES_HOST;
            public static final IRI POSTGRES_PORT;
            public static final IRI POSTGRES_DATABASE;
            public static final IRI POSTGRES_USER;
            public static final IRI POSTGRES_PASSWORD;
            
            static {
                POWDER_SAIL = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PowderSail");
                POSTGRES_HOST = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "postgresHost");
                POSTGRES_PORT = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "postgresPort");
                POSTGRES_DATABASE = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "postgresDatabase");
                POSTGRES_USER = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "postgresUser");
                POSTGRES_PASSWORD = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "postgresPassword");
            }
            
        }
        
    }
}
