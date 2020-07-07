package org.semagrow.test;

import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.semagrow.cli.CliMain;
import org.semagrow.query.SemagrowTupleQuery;
import org.semagrow.repository.impl.SemagrowSailRepository;
import org.semagrow.sail.SemagrowSail;
import org.semagrow.sail.config.SemagrowSailConfig;
import org.semagrow.sail.config.SemagrowSailFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CropsBenchTest {

    private static final Logger logger = LoggerFactory.getLogger(CliMain.class);

    public static void main(String[] args) {

        int lucas1[] = { 5245, 2887, 8653, 7357, 6112, 1768, 3939, 1618, 8518, 7203, 887, 6894, 4616 };
        int lucas2[] = { 716, 2542, 8538, 8061 };
        int lucas3[] = { 2266, 5798, 2091, 3114, 1193, 7768, 5046, 7708, 4413, 5451, 447 };

        for (int i: lucas1) {
            String[] argv = {"/tmp/repository.ttl", getQuery1(i), "/tmp/results-l1-" + i + ".json"};
            CliMain.main(argv);
            decompose(getQuery1(i));
        }

        for (int i: lucas2) {
            String[] argv = {"/tmp/repository.ttl", getQuery2(i), "/tmp/results-l2-" + i + ".json"};
            CliMain.main(argv);
            decompose(getQuery2(i));
        }

        for (int i: lucas3) {
            String[] argv = {"/tmp/repository.ttl", getQuery3(i), "/tmp/results-l3-" + i + ".json"};
            CliMain.main(argv);
            decompose(getQuery3(i));
        }
    }

    public static void decompose(String queryString) {
        try {
            SemagrowSailFactory factory = new SemagrowSailFactory();
            SemagrowSailConfig config = new SemagrowSailConfig();
            Repository repository = new SemagrowSailRepository((SemagrowSail) factory.getSail(config));
            repository.initialize();
            RepositoryConnection conn = repository.getConnection();
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            TupleExpr plan = ((SemagrowTupleQuery) query).getDecomposedQuery();

        } catch (RepositoryConfigException e) {
            e.printStackTrace();
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (TupleQueryResultHandlerException e) {
            e.printStackTrace();
        }
    }

    public static String getQuery1(int id) {
        return "" +
                "PREFIX lucas: <http://deg.iit.demokritos.gr/lucas/>\n" +
                "PREFIX lucas_r: <http://deg.iit.demokritos.gr/lucas/resource/>\n" +
                "PREFIX invekos: <http://deg.iit.demokritos.gr/invekos/>\n" +
                "PREFIX lictm: <http://deg.iit.demokritos.gr/>\n" +
                "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n" +
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n" +
                "PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>\n" +
                "\n" +
                "SELECT * WHERE {\n" +
                "  {\n" +
                "    SELECT * WHERE {\n" +
                "      lucas_r:" + id + " geo:hasGeometry ?l_geom_id .\n" +
                "      ?l_geom_id geo:asWKT ?l_geom .\n" +
                "      ?inv invekos:hasCropTypeNumber ?i_ctype .\n" +
                "      ?inv geo:hasGeometry ?i_geom_id .\n" +
                "      ?i_geom_id geo:asWKT ?i_geom .\n" +
                "      BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?distance) .\n" +
                "      FILTER(?distance < 10) .\n" +
                "    }\n" +
                "    ORDER BY ASC(?distance)\n" +
                "    LIMIT 1\n" +
                "  }\n" +
                "  lucas_r:" + id + " lucas:hasLC1 ?lc1 .\n" +
                "  lucas_r:" + id + " lucas:hasLC1_SPEC ?lc1_sp .\n" +
                "  ?c lictm:lucasLC1 ?lc1 .\n" +
                "  ?c lictm:lucasLC1_spec ?lc1_sp .\n" +
                "  ?c lictm:invekosCropTypeNumber ?l_ctype .\n" +
                "  FILTER(?l_ctype = ?i_ctype) .\n" +
                "}";
    }

    public static String getQuery2(int id) {
        return "" +
                "PREFIX lucas: <http://deg.iit.demokritos.gr/lucas/>\n" +
                "PREFIX lucas_r: <http://deg.iit.demokritos.gr/lucas/resource/>\n" +
                "PREFIX invekos: <http://deg.iit.demokritos.gr/invekos/>\n" +
                "PREFIX lictm: <http://deg.iit.demokritos.gr/>\n" +
                "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n" +
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n" +
                "PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>\n" +
                "SELECT * WHERE {\n" +
                "  {\n" +
                "    SELECT * WHERE {\n" +
                "      lucas_r:" + id + " geo:hasGeometry ?l_geom_id .\n" +
                "      ?l_geom_id geo:asWKT ?l_geom .\n" +
                "      ?inv invekos:hasCropTypeNumber ?i_ctype .\n" +
                "      ?inv geo:hasGeometry ?i_geom_id .\n" +
                "      ?i_geom_id geo:asWKT ?i_geom .\n" +
                "      BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?distance) .\n" +
                "      FILTER(?distance < 10) .\n" +
                "    }\n" +
                "    ORDER BY ASC(?distance)\n" +
                "    LIMIT 1\n" +
                "  }\n" +
                "  FILTER NOT EXISTS {\n" +
                "    lucas_r:" + id + " lucas:hasLC1 ?lc1 .\n" +
                "    lucas_r:" + id + " lucas:hasLC1_SPEC ?lc1_sp .\n" +
                "    ?c lictm:lucasLC1 ?lc1 .\n" +
                "    ?c lictm:lucasLC1_spec ?lc1_sp .\n" +
                "    ?c lictm:invekosCropTypeNumber ?l_ctype .\n" +
                "    FILTER(?l_ctype = ?i_ctype) . \n" +
                "  }\n" +
                "}";
    }

    public static String getQuery3(int id) {
        return "" +
                "PREFIX lucas: <http://deg.iit.demokritos.gr/lucas/>\n" +
                "PREFIX lucas_r: <http://deg.iit.demokritos.gr/lucas/resource/>\n" +
                "PREFIX invekos: <http://deg.iit.demokritos.gr/invekos/>\n" +
                "PREFIX lictm: <http://deg.iit.demokritos.gr/>\n" +
                "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n" +
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n" +
                "PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>\n" +
                "\n" +
                "SELECT * WHERE {\n" +
                "  lucas_r:" + id + " lucas:hasLC1 ?lc1 .\n" +
                "  lucas_r:" + id + " lucas:hasLC1_SPEC ?lc1_sp .\n" +
                "  ?c lictm:lucasLC1 ?lc1 .\n" +
                "  ?c lictm:lucasLC1_spec ?lc1_sp .\n" +
                "  {\n" +
                "    SELECT * WHERE {\n" +
                "      lucas_r:" + id + " geo:hasGeometry ?l_geom_id .\n" +
                "      ?l_geom_id geo:asWKT ?l_geom .\n" +
                "      ?inv invekos:hasCropTypeNumber ?i_ctype .\n" +
                "      ?inv geo:hasGeometry ?i_geom_id .\n" +
                "      ?i_geom_id geo:asWKT ?i_geom .\n" +
                "      \n" +
                "      BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?distance) .\n" +
                "      FILTER (?distance >= 0) .\n" +
                "    }\n" +
                "    ORDER BY ASC(?distance)\n" +
                "    LIMIT 1\n" +
                "  }\n" +
                "  FILTER(?distance >= 10) .\n" +
                "}";
    }
}
