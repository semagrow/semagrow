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

public class GdoPostgisExperiments extends TestCase {

	public void testSemagrowQuery() throws IOException {
		
		// Austria's MBB / 16
		String q4a = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"PREFIX invekospg: <http://rdf.semagrow.org/pgm/invekospg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  ?s2 rdf:type invekospg:geometry .\n" +
		        "  ?s2 geo:asWKT ?w2 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((15.25520182 48.356487275,15.25520182 49.01704407,17.16236115 49.01704407,17.16236115 48.356487275,15.25520182 48.356487275))')) .\n" +
		        "  FILTER (geof:distance(?w1, ?w2, uom:metre) < 10000) .\n" +
		        "}";
		
		String q4al = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((15.25520182 48.356487275,15.25520182 49.01704407,17.16236115 49.01704407,17.16236115 48.356487275,15.25520182 48.356487275))')) .\n" +
		        "}";

		// Austria's MBB / 8
		String q4b = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"PREFIX invekospg: <http://rdf.semagrow.org/pgm/invekospg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  ?s2 rdf:type invekospg:geometry .\n" +
		        "  ?s2 geo:asWKT ?w2 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((15.25520182 49.01704407,17.16236115 49.01704407,17.16236115 47.69593048,15.25520182 47.69593048,15.25520182 49.01704407))')) .\n" +
		        "  FILTER (geof:distance(?w1, ?w2, uom:metre) < 10000) .\n" +
		        "}";
		
		String q4bl = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((15.25520182 49.01704407,17.16236115 49.01704407,17.16236115 47.69593048,15.25520182 47.69593048,15.25520182 49.01704407))')) .\n" +
		        "}";
		
		// Austria's MBB / 4
		String q4c = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"PREFIX invekospg: <http://rdf.semagrow.org/pgm/invekospg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  ?s2 rdf:type invekospg:geometry .\n" +
		        "  ?s2 geo:asWKT ?w2 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((13.34804249 47.69593048,13.34804249 49.01704407,17.16236115 49.01704407,17.16236115 47.69593048,13.34804249 47.69593048))')) .\n" +
		        "  FILTER (geof:distance(?w1, ?w2, uom:metre) < 10000) .\n" +
		        "}";
		
		String q4cl = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((13.34804249 47.69593048,13.34804249 49.01704407,17.16236115 49.01704407,17.16236115 47.69593048,13.34804249 47.69593048))')) .\n" +
		        "}";
		
		// Austria's MBB / 2
		String q4d = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"PREFIX invekospg: <http://rdf.semagrow.org/pgm/invekospg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  ?s2 rdf:type invekospg:geometry .\n" +
		        "  ?s2 geo:asWKT ?w2 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((13.34804249 49.01704407,17.16236115 49.01704407,17.16236115 46.37481689,13.34804249 46.37481689,13.34804249 49.01704407))')) .\n" +
		        "  FILTER (geof:distance(?w1, ?w2, uom:metre) < 10000) .\n" +
		        "}";
		
		String q4dl = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((13.34804249 49.01704407,17.16236115 49.01704407,17.16236115 46.37481689,13.34804249 46.37481689,13.34804249 49.01704407))')) .\n" +
		        "}";
		
		// Austria's MBB
		String q4e = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"PREFIX invekospg: <http://rdf.semagrow.org/pgm/invekospg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  ?s2 rdf:type invekospg:geometry .\n" +
		        "  ?s2 geo:asWKT ?w2 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((9.53372383 46.37481689,9.53372383 49.01704407,17.16236115 49.01704407,17.16236115 46.37481689,9.53372383 46.37481689))')) .\n" +
		        "  FILTER (geof:distance(?w1, ?w2, uom:metre) < 10000) .\n" +
		        "}";
		
		String q4el = "" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"PREFIX lucaspg: <http://rdf.semagrow.org/pgm/lucaspg/>" +
				"SELECT * WHERE {\n" +
				"  ?s1 rdf:type lucaspg:geometry .\n" +
		        "  ?s1 geo:asWKT ?w1 .\n" +
		        "  FILTER (geof:sfWithin(?w1, 'POLYGON((9.53372383 46.37481689,9.53372383 49.01704407,17.16236115 49.01704407,17.16236115 46.37481689,9.53372383 46.37481689))')) .\n" +
		        "}";
		
		SemagrowSailFactory factory = new SemagrowSailFactory();
		SemagrowSailConfig config = new SemagrowSailConfig();
		Repository repo = new SemagrowSailRepository((SemagrowSail) factory.getSail(config));
		
		repo.initialize();
		        
		RepositoryConnection conn = repo.getConnection();
		
		TupleQuery query = conn.prepareTupleQuery(q4a);
		
		final int[] count = {0};
		final FileWriter writer = new FileWriter("/tmp/results.txt", false);
		
		TupleExpr plan = ((SemagrowTupleQuery) query).getDecomposedQuery();

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