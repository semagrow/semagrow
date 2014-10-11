package eu.semagrow.stack.modules.vocabulary;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

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
            public static final URI SPARQL_SAMPLE;
            /* Predicates */
            
            /*  http://schema.semagrow.eu/system/1.0.0/samples/sparqlSampleText */
            public static final URI SPARQL_SAMPLE_TEXT;
            
            static {
                SPARQL_SAMPLE = ValueFactoryImpl.getInstance().createURI(SEMAGROW.SYSTEM.SPARQL_SAMPLES.NAMESPACE, "SparqlSample");
                SPARQL_SAMPLE_TEXT = ValueFactoryImpl.getInstance().createURI(SEMAGROW.SYSTEM.SPARQL_SAMPLES.NAMESPACE, "sparqlSampleText"); 
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
            public static final URI POWDER_SAIL;
            
            /* PREDICATES */
            public static final URI POSTGRES_HOST;
            public static final URI POSTGRES_PORT;
            public static final URI POSTGRES_DATABASE;
            public static final URI POSTGRES_USER;
            public static final URI POSTGRES_PASSWORD;
            
            static {
                POWDER_SAIL = ValueFactoryImpl.getInstance().createURI(NAMESPACE, "PowderSail");
                POSTGRES_HOST = ValueFactoryImpl.getInstance().createURI(NAMESPACE, "postgresHost");
                POSTGRES_PORT = ValueFactoryImpl.getInstance().createURI(NAMESPACE, "postgresPort");
                POSTGRES_DATABASE = ValueFactoryImpl.getInstance().createURI(NAMESPACE, "postgresDatabase");
                POSTGRES_USER = ValueFactoryImpl.getInstance().createURI(NAMESPACE, "postgresUser");
                POSTGRES_PASSWORD = ValueFactoryImpl.getInstance().createURI(NAMESPACE, "postgresPassword");
            }
            
        }
        
    }
}
