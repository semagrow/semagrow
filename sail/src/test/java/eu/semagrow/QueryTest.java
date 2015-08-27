package eu.semagrow;

import eu.semagrow.commons.utils.FileUtils;
import eu.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.query.SemagrowTupleQuery;
import eu.semagrow.repository.SemagrowRepository;

import org.openrdf.model.Graph;
import org.openrdf.query.*;
import org.openrdf.query.resultio.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.config.*;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.config.SailConfigException;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;


/**
 * Created by angel on 22/8/2015.
 */
public class QueryTest extends TestCase
{
	private static final File repoConfigFile;
	static {
		try { repoConfigFile = FileUtils.getFile( "repository.ttl" ); }
		catch( IOException ex ) { throw new AssertionError( ex ); }
	}

	private static final String strQuery[] = {
"PREFIX dct: <http://purl.org/dc/terms/>\n" +
		"SELECT * WHERE { ?s rdf:type ?o } LIMIT 20"
	};
	private static final String pathnameDBPedia;
	static {
		try { pathnameDBPedia = FileUtils.getFile( "dbpedia.ttl" ).getAbsolutePath(); }
		catch( IOException ex ) { throw new AssertionError( ex ); }
	}

	private static final String strQueryFAO[] = {
"PREFIX dct: <http://purl.org/dc/terms/>\n" +
		"SELECT distinct ?s (COUNT(distinct ?o) as ?NELEMENTS) WHERE {\n" +
		"  <http://agris.fao.org/aos/records/PH2011000084> dct:subject ?o .\n" +
		"  ?s dct:subject ?o .\n" +
		"  ?s rdf:type <http://semagrow.eu/rdf#CrawledDocument> .\n" +
		"} \n" +
		"GROUP BY ?s \n" +
		"ORDER BY DESC(?NELEMENTS)\n" +
		"LIMIT 20 ",
"PREFIX dct: <http://purl.org/dc/terms/>\n" +
		"PREFIX s: <http://schema.semagrow.eu/>\n" +
		"SELECT  ?s ?o (s:provenance() as ?p) WHERE {\n" +
		"  <http://agris.fao.org/aos/records/PH2011000084> dct:subject ?o .\n" +
		"  ?s dct:subject ?o .\n" +
		"  ?s rdf:type <http://semagrow.eu/rdf#CrawledDocument> .\n" +
		"} \n "
	};

	private static final String strQueryDLO[] = {
"SELECT *\n" +
		"WHERE {\n" +
		"<http://semagrow.eu/rdf/struct/epic_hadgem2-es_rcp2p6_ssp2_co2_firr_yield_whe_annual_2005_2099_struct>\n" +
		"<http://purl.org/linked-data/cube#component> ?LON_COMP .\n" +
		"?LON_COMP <http://semagrow.eu/rdf/data/standard_name>\n" +
		"\"longitude\"^^<http://www.w3.org/2001/XMLSchema#string> .\n" +
		"?LON_COMP <http://purl.org/linked-data/cube#codeList> ?CODELIST .\n" +
		"}",
"SELECT ?WKT1 ?WKT2 (<http://strdf.di.uoa.gr/ontology#:distance>(?WKT1, ?WKT2,\n" +
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
		"}"
};

    public void testQuery()
    throws Exception
    {

        // remove CSV and TSV format due to bug: literals are recognized as URIs if they contain a substring parsable as URI.
        TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();
        registry.remove(registry.get(TupleQueryResultFormat.CSV));
        registry.remove(registry.get(TupleQueryResultFormat.TSV));
        registry.remove(registry.get(TupleQueryResultFormat.JSON));

        BooleanQueryResultParserRegistry booleanRegistry = BooleanQueryResultParserRegistry.getInstance();
        booleanRegistry.remove(booleanRegistry.get(BooleanQueryResultFormat.JSON));

        SemagrowRepositoryConfig repoConfig = getConfig( QueryTest.repoConfigFile );
        String repoType = repoConfig.getType();
        RepositoryFactory repoFactory = RepositoryRegistry.getInstance().get( repoType );
        Repository repository = (SemagrowRepository)repoFactory.getRepository( repoConfig );
        repository.initialize();
        RepositoryConnection conn = repository.getConnection();
        
        SemagrowTupleQuery query =
        		(SemagrowTupleQuery) conn.prepareTupleQuery(QueryLanguage.SPARQL, QueryTest.strQueryDLO[0] );

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

    private SemagrowRepositoryConfig getConfig( File file )
    {

        try {
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

    protected Graph parseConfig(File file)
    throws SailConfigException, IOException
    {

    	org.openrdf.rio.RDFFormat format =
    			org.openrdf.rio.Rio.getParserFormatForFileName( file.getAbsolutePath() );
        if( format==null ) {
            throw new SailConfigException("Unsupported file format: " + file.getAbsolutePath());
        }
        org.openrdf.rio.RDFParser parser = org.openrdf.rio.Rio.createParser(format);
        Graph model = new org.openrdf.model.impl.GraphImpl();
        parser.setRDFHandler(new StatementCollector(model));
        InputStream stream = new FileInputStream(file);

        try {
            parser.parse( stream, file.getAbsolutePath() );
        } catch (Exception e) {
            throw new SailConfigException("Error parsing file!");
        }

        stream.close();
        return model;
    }
}
