package org.semagrow.test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.semagrow.query.SemagrowTupleQuery;
import org.semagrow.repository.impl.SemagrowSailRepository;
import org.semagrow.sail.SemagrowSail;
import org.semagrow.sail.config.SemagrowSailConfig;
import org.semagrow.sail.config.SemagrowSailFactory;

import junit.framework.TestCase;

public class SemagrowFinalQueriesTest extends TestCase {

	public void testSemagrowQuery() throws IOException {
		
		int id_q1 = 5245, id_q2 = 2542, id_q3 = 7708;
		
		String q1 = "" +
				"PREFIX lucas: <http://deg.iit.demokritos.gr/lucas/>" +
				"PREFIX lucas_r: <http://deg.iit.demokritos.gr/lucas/resource/>" +
				"PREFIX invekos: <http://deg.iit.demokritos.gr/invekos/>" +
				"PREFIX invekos_r: <http://deg.iit.demokritos.gr/invekos/resource/>" +
				"PREFIX lictm: <http://deg.iit.demokritos.gr/>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  {\n" +
				"    SELECT * WHERE {\n" +
				"      lucas_r:" + id_q1 + " geo:hasGeometry ?l_geom_id .\n" +
				"      ?l_geom_id geo:asWKT ?l_geom .\n" +
				"      ?inv invekos:hasCropTypeNumber ?i_ctype .\n" +
				"      ?inv geo:hasGeometry ?i_geom_id .\n" +
				"      ?i_geom_id geo:asWKT ?i_geom .\n" +
//				"      BIND(geof:distance(?l_geom,?i_geom,opengis:degree) as ?distance) .\n" +
				"      BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?distance) .\n" +
				"      FILTER(?distance < 10) .\n" +
				"    }\n" +
				"    ORDER BY ASC(?distance)\n" +
				"    LIMIT 1\n" +
				"  }\n" +
				"  lucas_r:" + id_q1 + " lucas:hasLC1 ?lc1 .\n" +
				"  lucas_r:" + id_q1 + " lucas:hasLC1_SPEC ?lc1_sp .\n" +
				"  ?c lictm:lucasLC1 ?lc1 .\n" +
				"  ?c lictm:lucasLC1_spec ?lc1_sp .\n" +
				"  ?c lictm:invekosCropTypeNumber ?l_ctype .\n" +
				"  FILTER(?l_ctype = ?i_ctype) .\n" +
				"}";
		
		
		String q2 = "" +
				"PREFIX lucas: <http://deg.iit.demokritos.gr/lucas/>" +
				"PREFIX lucas_r: <http://deg.iit.demokritos.gr/lucas/resource/>" +
				"PREFIX invekos: <http://deg.iit.demokritos.gr/invekos/>" +
				"PREFIX invekos_r: <http://deg.iit.demokritos.gr/invekos/resource/>" +
				"PREFIX lictm: <http://deg.iit.demokritos.gr/>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  {\n" +
				"    SELECT * WHERE {\n" +
				"      lucas_r:" + id_q2 + " geo:hasGeometry ?l_geom_id .\n" +
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
				"    lucas_r:" + id_q2 + " lucas:hasLC1 ?lc1 .\n" +
				"    lucas_r:" + id_q2 + " lucas:hasLC1_SPEC ?lc1_sp .\n" +
				"    ?c lictm:lucasLC1 ?lc1 .\n" +
				"    ?c lictm:lucasLC1_spec ?lc1_sp .\n" +
				"    ?c lictm:invekosCropTypeNumber ?l_ctype .\n" +
				"    FILTER(?l_ctype = ?i_ctype) .\n" +
				"  }\n" +
				"}";
		
		
		String q3 = "" +
				"PREFIX lucas: <http://deg.iit.demokritos.gr/lucas/>\n" +
				"PREFIX lucas_r: <http://deg.iit.demokritos.gr/lucas/resource/>\n" +
				"PREFIX invekos: <http://deg.iit.demokritos.gr/invekos/>\n" +
				"PREFIX lictm: <http://deg.iit.demokritos.gr/>\n" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\n" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>\n" +
				"\n" +
				"SELECT * WHERE {\n" +
				"  lucas_r:" + id_q3 + " lucas:hasLC1 ?lc1 .\n" +
				"  FILTER NOT EXISTS {\n" +
				"    lucas_r:" + id_q3 + " geo:hasGeometry ?l_geom_id .\n" +
				"    ?l_geom_id geo:asWKT ?l_geom .\n" +
				"    ?inv invekos:hasCropTypeNumber ?i_ctype .\n" +
				"    ?inv geo:hasGeometry ?i_geom_id .\n" +
				"    ?i_geom_id geo:asWKT ?i_geom .\n" +
				"    FILTER (geof:distance(?l_geom,?i_geom,opengis:metre) < 10) .\n" +
				"  }\n" +
				"}";
		
		
		SemagrowSailFactory factory = new SemagrowSailFactory();
		SemagrowSailConfig config = new SemagrowSailConfig();
		Repository repo = new SemagrowSailRepository((SemagrowSail) factory.getSail(config));
		
		repo.initialize();
		        
		RepositoryConnection conn = repo.getConnection();
		
		TupleQuery query = conn.prepareTupleQuery(q1);
		
		final int[] count = {0};
		final FileWriter writer = new FileWriter("/tmp/results.txt", false);
		
		TupleExpr plan = ((SemagrowTupleQuery) query).getDecomposedQuery();
		//writer.write(qq);
		//writer.write("\n");
//		System.out.println(EndpointCollector.process(plan));

		if (true) {
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
				
				}
				
				@Override
				public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
					try {
						writer.write(bindingSet.toString());
						writer.write("\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
					count[0]++;
				}
			});
		}
    
		writer.close();
		
	}
}