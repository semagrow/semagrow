package eu.semagrow.utils.sparqlutils;

import eu.semagrow.commons.CONSTANTS;
import java.util.HashMap;
import java.util.HashSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.repository.sail.SailBooleanQuery;
import org.eclipse.rdf4j.repository.sail.SailGraphQuery;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class SparqlUtils {
    
    public static final String QUERYTYPE_TUPLEQUERY = SailTupleQuery.class.getName();
    public static final String QUERYTYPE_GRAPHQUERY = SailGraphQuery.class.getName();
    public static final String QUERYTYPE_BOOLEANQUERY = SailBooleanQuery.class.getName();    

    private static final HashMap<String,HashSet<String>> acceptPosibilities = new HashMap<String,HashSet<String>>();
    private static final HashMap<String,String> defaultContentTypes = new HashMap<String,String>();
    
    static {
        acceptPosibilities.put(QUERYTYPE_TUPLEQUERY, new HashSet<String>());
        acceptPosibilities.put(QUERYTYPE_GRAPHQUERY, new HashSet<String>());
        acceptPosibilities.put(QUERYTYPE_BOOLEANQUERY, new HashSet<String>());

        acceptPosibilities.get(QUERYTYPE_TUPLEQUERY).add(CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        acceptPosibilities.get(QUERYTYPE_TUPLEQUERY).add(CONSTANTS.MIMETYPES.SPARQLRESULTS_XML);
        acceptPosibilities.get(QUERYTYPE_TUPLEQUERY).add(CONSTANTS.MIMETYPES.TEXT_HTML);

        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_RDFXML);
        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_N3);
        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_TURTLE);
        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_TRIG);
        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_TRIX);
        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_JSONLD);
        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_NQUADS);
        acceptPosibilities.get(QUERYTYPE_GRAPHQUERY).add(CONSTANTS.MIMETYPES.RDF_NTRIPLES);

        defaultContentTypes.put(QUERYTYPE_TUPLEQUERY, CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON);
        defaultContentTypes.put(QUERYTYPE_GRAPHQUERY, CONSTANTS.MIMETYPES.RDF_TURTLE); 
        defaultContentTypes.put(QUERYTYPE_BOOLEANQUERY, CONSTANTS.MIMETYPES.TEXT_PLAIN); 
    }
 
    /**
     * Method to get a correct mimeType for a given query. 
     * 
     * @param query
     * @param acceptMimeType
     * @return the given mimetype if suitable for the query or the default mimetype for 
     * the given query in case the acceptMimeType is not suitable.
     */
    public static String getAcceptMimeType(Query query, String acceptMimeType){
        String queryType = QUERYTYPE_TUPLEQUERY;
        if((query instanceof GraphQuery)){
            queryType = QUERYTYPE_GRAPHQUERY;
        }
        if(query instanceof BooleanQuery){
            queryType = QUERYTYPE_BOOLEANQUERY;
        }
        if(acceptPosibilities.get(queryType).contains(acceptMimeType)){
            return acceptMimeType;
        } else {
            return defaultContentTypes.get(queryType);
        }
    }
    
}
