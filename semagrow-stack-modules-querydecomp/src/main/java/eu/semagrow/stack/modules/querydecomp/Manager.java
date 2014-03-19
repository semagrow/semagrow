package eu.semagrow.stack.modules.querydecomp;

import eu.semagrow.stack.modules.querydecomp.sail.SemagrowSail;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.sail.SailException;

import java.util.List;

/**
 * Created by angel on 3/12/14.
 */
public class Manager {

    static private Repository getRepository() throws SailException {
        SemagrowSail sail = new SemagrowSail();
        sail.initialize();
        return new SailRepository(sail);
    }


    static TupleQueryResult doQuery(String sparqlQuery) throws Exception {
        Repository repo = getRepository();
        RepositoryConnection conn = repo.getConnection();
        TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
        return q.evaluate();
    }


    final static String PREFIXES = "PREFIX dc:<http://purl.org/dc/elements/1.1/>\n" +
            "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX owl:<http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rss:<http://purl.org/rss/1.0/>\n" +
            "PREFIX ntnames:<http://semanticbible.org/ns/2006/NTNames.owl#>\n" +
            "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>\n" +
            "PREFIX ntind:<http://www.semanticbible.org/2005/09/NTN-individuals.owl#>\n" +
            "PREFIX ntonto:<http://semanticbible.org/ns/2006/NTNames#>\n" +
            "PREFIX dbo:<http://dbpedia.org/ontology/>\n" +
            "PREFIX dbpedia:<http://dbpedia.org/resource/>\n";


    final static String q1 = "SELECT ?urlz WHERE {\n" +
            "?s <http://www.semagrow.eu/schema/farms/cultivationType> \"peanuts\".\n" +
            "?s <http://www.semagrow.eu/schema/farms/yield> ?y.\n" +
            "?s <http://www.semagrow.eu/schema/biblios/URL> ?urlz.\n" +
            "FILTER regex(?y,\"80\")\n" +
            "} LIMIT 100";

    final static String BIG_QUERY = "PREFIX  farm: <http://ontologies.seamless-ip.org/farm.owl#>\n" +
            "PREFIX  dc:   <http://purl.org/dc/terms/>\n" +
            "PREFIX  wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
            "PREFIX  t4f:  <http://semagrow.eu/schemas/t4f#>\n" +
            "PREFIX  laflor: <http://semagrow.eu/schemas/laflor#>\n" +
            "PREFIX  eururalis: <http://semagrow.eu/schemas/eururalis#>\n" +
            "PREFIX  crop: <http://ontologies.seamless-ip.org/crop.owl#>\n" +
            "\n" +
            "SELECT  ?Longitude ?Latitude ?U\n" +
            "WHERE\n" +
            "  { { SELECT  ?Longitude ?Latitude (avg(?PR) AS ?PRE)\n" +
            "      WHERE\n" +
            "        { ?R t4f:hasLong ?Longitude .\n" +
            "          ?R t4f:hasLat ?Latitude .\n" +
            "          ?R eururalis:landuse \"11\" .\n" +
            "          ?R t4f:precipitation ?PR\n" +
            "        }\n" +
            "      GROUP BY ?Longitude ?Latitude\n" +
            "    }\n" +
            "    ?F farm:year 2010 .\n" +
            "    ?F farm:cropinformation ?C .\n" +
            "    ?C crop:productionmushrooms ?M .\n" +
            "    ?F farm:agrienvironmentalzone ?A .\n" +
            "    ?A farm:longitude ?ALONG .\n" +
            "    ?A farm:latitude ?ALAT\n" +
            "    { SELECT  ?A (avg(?PR) AS ?PRE2)\n" +
            "      WHERE\n" +
            "        { ?A farm:dailyclimate ?D .\n" +
            "          ?D farm:rainfall ?PR\n" +
            "        }\n" +
            "      GROUP BY ?A\n" +
            "    }\n" +
            "    ?J dc:subject <http://aims.fao.org/aos/agrovoc/xl_en_1299487055215> .\n" +
            "    ?J laflor:location ?U .\n" +
            "    ?J laflor:language <http://id.loc.gov/vocabulary/iso639-2/es> .\n" +
            "    ?J <http://schema.org/about> ?P .\n" +
            "    ?P wgs84_pos:lat ?ALAT2 .\n" +
            "    ?P wgs84_pos:long ?ALONG2\n" +
            "    FILTER ( ( ( ( ( ( ( ?M > 10 ) && ( ( ?PRE - ?PRE2 ) < 1 ) ) && ( ( ?PRE - ?PRE2 ) > -1 ) ) && ( ( ?ALAT - ?ALAT2 ) < 1 ) ) && ( ( ?ALAT - ?ALAT2 ) > -1 ) ) && ( ?Longitude < 42.02 ) ) && ( ?Latitude < 1.667 ) )\n" +
            "  }";

    public static void main(String[] saArgs) {

        //org.apache.log4j.BasicConfigurator.configure();

        String query = PREFIXES +
            "\n" +
            "SELECT ?city ?cityName WHERE\n" +
            "{\n" +
            " {?city rdf:type ntonto:City}\n" +
            " {?city rdfs:label ?cityName}\n" +
            "} LIMIT 10";

        String query2 = PREFIXES +
            "\n" +
            "SELECT ?x WHERE {?x rdf:type rdfs:Class. ?y rdf:type ?z. ?x rdf:type ?y. FILTER (?y = ?z) } LIMIT 10";


        try {
            TupleQueryResult q = doQuery(q1);
            System.out.print("success");

            while (q.hasNext())
            {
                BindingSet b  = q.next();
                for (String name : q.getBindingNames())
                    System.out.print(b.getBinding(name));

                System.out.print("\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

