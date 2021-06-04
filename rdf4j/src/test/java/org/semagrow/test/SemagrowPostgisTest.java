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

public class SemagrowPostgisTest extends TestCase {

	public void testSemagrowQuery() throws IOException {
		
		String q1 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geom: <http://rdf.semagrow.org/pgm/conn1/resource/>" +
				"SELECT * WHERE {\n" +
				"  geom:9 geo:asWKT ?wkt .\n" +
				"}";
		
		String q2 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"SELECT * WHERE {\n" +
				"  ?geom geo:asWKT ?wkt .\n" +
				"}";
		
		String q3 = "" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX pgm: <http://rdf.semagrow.org/pgm/conn1/>" +
				"SELECT * WHERE {\n" +
				"  ?g1 rdf:type ?pgm1 .\n" + 
				"}";
		
		String q4 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX pgm: <http://rdf.semagrow.org/pgm/conn1/>" +
				"SELECT * WHERE {\n" +
				"  ?g1 rdf:type ?pgm .\n" + 
				"  ?g1 geo:asWKT 'POLYGON((15.766116465006913 47.468846655138925,15.766023858378992 47.468820560656525,15.765813297126561 47.469274498228636,15.766037997425707 47.469347481399765,15.766535743496608 47.46952552064175,15.767193339682079 47.46971920469918,15.767909886775033 47.46995517575107,15.76860324687627 47.470312448775175,15.768762622151753 47.47037848705451,15.769075742791122 47.46979399909336,15.768245698110524 47.46953792220506,15.76742328440326 47.46931209479831,15.766914596494008 47.4691619272585,15.766116465006913 47.468846655138925))' .\n" + 
				"}";
		
		String q5 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX pgm: <http://rdf.semagrow.org/pgm/conn1/>" +
				"SELECT * WHERE {\n" +
				"  ?g1 rdf:type pgm:geometry .\n" + 
				"  ?g1 geo:asWKT ?w1 .\n" + 
				"}";
		
		String q6 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX pgm2: <http://rdf.semagrow.org/pgm/conn2/>" +
				"PREFIX geom1: <http://rdf.semagrow.org/pgm/conn1/resource/>" +
				"SELECT * WHERE {\n" +
//				"  ?g1 rdf:type pgm:geometry .\n" + 
				"  geom1:5 geo:asWKT ?w1 .\n" + 
				"  ?g2 rdf:type pgm2:geometry .\n" + 
				"  ?g2 geo:asWKT ?w2 .\n" + 
				"  FILTER(geof:distance(?w1,?w2,uom:metre) < 1000000) .\n" +
				"}";
		
		String q7 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX pgm1: <http://rdf.semagrow.org/pgm/conn1/>" +
				"PREFIX pgm2: <http://rdf.semagrow.org/pgm/conn2/>" +
				"SELECT * WHERE {\n" +
				"  <http://rdf.semagrow.org/pgm/conn1/resource/9> geo:asWKT ?w1 .\n" + 
				"  <http://rdf.semagrow.org/pgm/conn2/resource/101> geo:asWKT ?w2 .\n" + 
//				"  FILTER(geof:distance(?w1,?w2,uom:metre) < 1) .\n" +
				"}";
		
		String q8 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX pgm1: <http://rdf.semagrow.org/pgm/conn1/>" +
				"PREFIX pgm2: <http://rdf.semagrow.org/pgm/conn2/>" +
				"SELECT * WHERE {\n" +
				"  ?g1 rdf:type pgm1:geometry .\n" + 
				"  ?g2 rdf:type pgm2:geometry .\n" + 
				"  ?g1 geo:asWKT ?w1 .\n" + 
				"  ?g2 geo:asWKT ?w2 .\n" + 
//				"  FILTER regex(?g1, \"<http://rdf.semagrow.org/pgm/conn1/resource/9>\") .\n" +
//				"  FILTER regex(?g2, \"<http://rdf.semagrow.org/pgm/conn2/resource/101\") .\n" +
				"  FILTER(str(?g2) = \"http://rdf.semagrow.org/pgm/conn2/resource/101\") .\n" +
				"  FILTER(str(?g1) = \"http://rdf.semagrow.org/pgm/conn1/resource/9\") .\n" +
				"  FILTER(geof:distance(?w1,?w2,uom:metre) < 1000000) .\n" +
//				"  FILTER(geof:distance(?w1,?w2,uom:metre) = 10) .\n" +
				"}";
		
		String q9 = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX pgm1: <http://rdf.semagrow.org/pgm/conn1/>" +
				"PREFIX pgm2: <http://rdf.semagrow.org/pgm/conn2/>" +
				"PREFIX geom1: <http://rdf.semagrow.org/pgm/conn1/resource/>" +
				"  SELECT * WHERE {\n" +
				"  ?s2 rdf:type pgm2:geometry .\n" + 
				"  ?s1 geo:asWKT ?w1 .\n" + 
				"  ?s2 geo:asWKT ?w2 .\n" + 
				"  FILTER (geof:distance(?w1,?w2,uom:metre) < 100) .\n" + 
//				"} VALUES ?s1 { geom1:5, geom1:9 }";
				"}";

		
		
		SemagrowSailFactory factory = new SemagrowSailFactory();
		SemagrowSailConfig config = new SemagrowSailConfig();
		Repository repo = new SemagrowSailRepository((SemagrowSail) factory.getSail(config));
		
		repo.initialize();
		        
		RepositoryConnection conn = repo.getConnection();
		
		TupleQuery query = conn.prepareTupleQuery(q9);
		
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
		
//		assertEquals(37, count[0]);
	}
}