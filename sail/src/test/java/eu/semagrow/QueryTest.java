package eu.semagrow;

import eu.semagrow.commons.utils.FileUtils;
import eu.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.query.SemagrowTupleQuery;
import eu.semagrow.repository.SemagrowRepository;

import org.eclipse.rdf4j.OpenRDFException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.resultio.*;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.config.SailConfigException;

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
		pathnameDBPedia = null;
		/*
		try { pathnameDBPedia = FileUtils.getFile( "dbpedia.ttl" ).getAbsolutePath(); }
		catch( IOException ex ) { throw new AssertionError( ex ); }
		*/
	}

	private static final String strQueryFAO[] = {
"PREFIX dct: <http://purl.org/dc/terms/>\n" +
		"PREFIX s: <http://schema.semagrow.eu/>\n" +
		"SELECT * WHERE {\n" +
		"  <http://agris.fao.org/aos/records/PH2011000084> dct:subject ?SUBJ .\n" +
		"  ?SUBJ ?P ?O .\n" +
		"} \n ",
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

	protected SemagrowRepository repo = null;
	protected RepositoryConnection conn = null;

	protected void setUp()
	{
        // remove CSV and TSV format due to bug: literals are recognized as URIs if they contain a substring parsable as URI.
        TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();
        registry.remove(registry.get(TupleQueryResultFormat.CSV).get());
        registry.remove(registry.get(TupleQueryResultFormat.TSV).get());
        registry.remove(registry.get(TupleQueryResultFormat.JSON).get());

        BooleanQueryResultParserRegistry booleanRegistry = BooleanQueryResultParserRegistry.getInstance();
        booleanRegistry.remove(booleanRegistry.get(BooleanQueryResultFormat.JSON).get());

		try {
	        SemagrowRepositoryConfig repoConfig = getConfig( QueryTest.repoConfigFile );
	        String repoType = repoConfig.getType();
	        RepositoryFactory repoFactory = RepositoryRegistry.getInstance().get( repoType ).get();
			this.repo = (SemagrowRepository)repoFactory.getRepository( repoConfig );
			this.repo.initialize();
			this.conn = this.repo.getConnection();
		}
		catch( RepositoryConfigException ex ) {
			ex.printStackTrace();
		}
		catch( RepositoryException ex ) {
			ex.printStackTrace();
		}
		catch( OpenRDFException ex ) {
			ex.printStackTrace();
		}
		catch( IOException ex ) {
			ex.printStackTrace();
		}
	}

    private SemagrowRepositoryConfig getConfig( File file )
    throws OpenRDFException, IOException
    {
    	Model configGraph = parseConfig(file);
    	RepositoryConfig repConf = RepositoryConfig.create(configGraph, null);
    	repConf.validate();
    	RepositoryImplConfig implConf = repConf.getRepositoryImplConfig();
    	return (SemagrowRepositoryConfig)implConf;
    }

    protected Model parseConfig(File file)
    throws SailConfigException, IOException
    {
    	org.eclipse.rdf4j.rio.RDFFormat format =
    			org.eclipse.rdf4j.rio.Rio.getParserFormatForFileName( file.getAbsolutePath() ).get();
        if( format==null ) {
            throw new SailConfigException("Unsupported file format: " + file.getAbsolutePath());
        }
        org.eclipse.rdf4j.rio.RDFParser parser = org.eclipse.rdf4j.rio.Rio.createParser(format);
        Model model = new LinkedHashModel();
        parser.setRDFHandler(new StatementCollector(model));
        InputStream stream = new FileInputStream(file);

        try { parser.parse( stream, file.getAbsolutePath() ); }
        catch( Exception ex ) {
            throw new SailConfigException("Error parsing file!");
        }

        stream.close();
        return model;
    }

	public void testQuery()
    throws Exception
    {
		// Fixture setup must succeed
		assertFalse( this.repo == null );
		assertFalse( this.conn == null );

		String queryString = QueryTest.strQueryFAO[0];
		org.slf4j.Logger logger =
				org.slf4j.LoggerFactory.getLogger( QueryTest.class );
		logger.info( "Testing on {}", QueryTest.strQueryFAO[0] );

        SemagrowTupleQuery query =
        		(SemagrowTupleQuery) conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString );

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

    }


	@Override
	protected void tearDown()
	{
        try {
			this.conn.close();
	        this.repo.shutDown();
		}
		catch( RepositoryException ex ) {
			ex.printStackTrace();
		}
	}

}
