import eu.semagrow.commons.utils.FileUtils;
import eu.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.query.SemagrowTupleQuery;
import eu.semagrow.repository.SemagrowRepository;
import junit.framework.TestCase;
import org.eclipse.rdf4j.model.Graph;
import org.eclipse.rdf4j.model.impl.GraphImpl;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultParserRegistry;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParserRegistry;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.*;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import reactor.Environment;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by angel on 22/8/2015.
 */
public class SimpleQueryTest extends TestCase {

    public void testQuery() throws Exception {

        // remove CSV and TSV format due to bug: literals are recognized as URIs if they contain a substring parsable as URI.
        TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();
        registry.remove(registry.get(TupleQueryResultFormat.CSV));
        registry.remove(registry.get(TupleQueryResultFormat.TSV));
        registry.remove(registry.get(TupleQueryResultFormat.JSON));

        BooleanQueryResultParserRegistry booleanRegistry = BooleanQueryResultParserRegistry.getInstance();
        booleanRegistry.remove(booleanRegistry.get(BooleanQueryResultFormat.JSON));

        SemagrowRepositoryConfig repoConfig = getConfig();
        RepositoryFactory repoFactory = RepositoryRegistry.getInstance().get(repoConfig.getType());
        Repository repository = (SemagrowRepository) repoFactory.getRepository(repoConfig);
        repository.initialize();

        RepositoryConnection conn = repository.getConnection();

        String queryString = "PREFIX dct: <http://purl.org/dc/terms/>\n" +
                "SELECT distinct ?s (COUNT(distinct ?o) as ?NELEMENTS) WHERE {\n" +
                "  <http://agris.fao.org/aos/records/PH2011000084> dct:subject ?o .\n" +
                "  ?s dct:subject ?o .\n" +
                "  ?s rdf:type <http://semagrow.eu/rdf#CrawledDocument> .\n" +
                "} \n" +
                "GROUP BY ?s \n" +
                "ORDER BY DESC(?NELEMENTS)\n" +
                "LIMIT 20 ";

        String queryString1 = "PREFIX dct: <http://purl.org/dc/terms/>\n" +
                "PREFIX s: <http://schema.semagrow.eu/>\n" +
                "SELECT  ?s ?o (s:provenance() as ?p) WHERE {\n" +
                "  <http://agris.fao.org/aos/records/PH2011000084> dct:subject ?o .\n" +
                "  ?s dct:subject ?o .\n" +
                "  ?s rdf:type <http://semagrow.eu/rdf#CrawledDocument> .\n" +
                "} \n ";


        String queryString2  = "SELECT ?WKT1 ?WKT2 (<http://strdf.di.uoa.gr/ontology#:distance>(?WKT1, ?WKT2,\n" +
                "  \"http://www.opengis.net/def/uom/OGC/1.0/metre\"^^<http://www.w3.org/2001/XMLSchema#anyURI>)\n" +
                "  as ?DIST)\n" +
                "WHERE {\n" +
                "<http://semagrow.eu/rdf/struct/epic_hadgem2-es_rcp2p6_ssp2_co2_firr_yield_whe_annual_2005_2099_struct>\n" +
                "<http://purl.org/linked-data/cube#component> ?LON_COMP .\n" +
                "?LON_COMP <http://semagrow.eu/rdf/data/standard_name>\n" +
                "\"longitude\"^^<http://www.w3.org/2001/XMLSchema#string> .\n" +
                "?LON_COMP <http://purl.org/linked-data/cube#codeList> ?CODELIST .\n" +
                "?CODELIST <http://purl.org/linked-data/cube#hierarchyRoot> ?LON_POINT .\n" +
                "?LON_POINT <http://www.semagrow.eu/stabon/lon> ?WKT1 .\n" +
                "<http://www.semagrow.eu/agmip#expA1>\n" +
                "<http://www.w3.org/2003/01/geo/wgs84_pos#geometry> ?WKT2 .\n" +
                "}";


        String queryString3 = "SELECT ?s ?a WHERE { \n" +
                "?s <http://www.semagrow.eu/rdf/year> ?a. \n" +
                "?s <http://purl.org/dc/terms/type> \"Publication\".\n" +
                " FILTER( ?a > 1926).\n" +
                " FILTER( ?a < 2015).\n" +
                "} LIMIT 10";

        SemagrowTupleQuery query = (SemagrowTupleQuery) conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString3);


        System.out.println(query.getDecomposedQuery());

        query.evaluate(new TupleQueryResultHandler() {
            @Override
            public void handleBoolean(boolean b) throws QueryResultHandlerException {

            }

            @Override
            public void handleLinks(List<String> list) throws QueryResultHandlerException {

            }

            @Override
            public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {

            }

            @Override
            public void endQueryResult() throws TupleQueryResultHandlerException {
                System.out.println("end of results");
            }

            @Override
            public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
                System.out.println(bindingSet);
            }
        });


        conn.close();

    }

    public void testReactor() throws Exception {
        Environment.initialize();

        Streams
                .range(1, 10).concatMap(x -> Streams.just(x))
                .consume(x -> System.out.println(x.toString()));
    }


    private SemagrowRepositoryConfig getConfig() {

        try {
            File file = FileUtils.getFile("repository.ttl");
            Graph configGraph = parseConfig(file);
            RepositoryConfig repConf = RepositoryConfig.create(configGraph, null);
            repConf.validate();
            RepositoryImplConfig implConf = repConf.getRepositoryImplConfig();
            return (SemagrowRepositoryConfig)implConf;
        } catch (RepositoryConfigException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        } catch (SailConfigException | IOException | NullPointerException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        }
    }

    protected Graph parseConfig(File file) throws SailConfigException, IOException {

        RDFFormat format = Rio.getParserFormatForFileName(file.getAbsolutePath());
        if (format==null)
            throw new SailConfigException("Unsupported file format: " + file.getAbsolutePath());
        RDFParser parser = Rio.createParser(format);
        Graph model = new GraphImpl();
        parser.setRDFHandler(new StatementCollector(model));
        InputStream stream = new FileInputStream(file);

        try {
            parser.parse(stream, file.getAbsolutePath());
        } catch (Exception e) {
            throw new SailConfigException("Error parsing file!");
        }

        stream.close();
        return model;
    }
}
