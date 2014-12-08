package eu.semagrow.stack.modules.commons;

import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class CONSTANTS {
    public static class MIMETYPES {        
        public static final String SPARQLRESULTS_XML=TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
        public static final String SPARQLRESULTS_JSON=TupleQueryResultFormat.JSON.getDefaultMIMEType();
        public static final String RDF_RDFXML=RDFFormat.RDFXML.getDefaultMIMEType();
        public static final String RDF_TURTLE=RDFFormat.TURTLE.getDefaultMIMEType();
        public static final String RDF_TRIG=RDFFormat.TRIG.getDefaultMIMEType();
        public static final String RDF_TRIX=RDFFormat.TRIX.getDefaultMIMEType();
        public static final String RDF_N3=RDFFormat.N3.getDefaultMIMEType();
        public static final String RDF_JSONLD=RDFFormat.JSONLD.getDefaultMIMEType();
        public static final String RDF_NQUADS=RDFFormat.NQUADS.getDefaultMIMEType();
        public static final String RDF_NTRIPLES=RDFFormat.NTRIPLES.getDefaultMIMEType();        
        public static final String TEXT_HTML="text/html";
        public static final String TEXT_PLAIN="text/plain";
        
    }
    public static class WEBAPP {
        public static final String PARAM_QUERY = "query";
        public static final String PARAM_QUERY_TXT = "query_txt";
        public static final String PARAM_ACCEPT = "accept";
        public static final String PARAM_TEMPLATE = "template";
        
        public static class ROLES {
            public static final String ROLE_SEMAGROW_ADMIN = "SemaGrowAdmin";
            public static final String ROLE_SEMAGROW_USER = "SemaGrowUser";
        }
    }
}
