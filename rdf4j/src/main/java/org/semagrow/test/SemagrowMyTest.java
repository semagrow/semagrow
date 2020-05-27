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
//import org.semagrow.plan.util.EndpointCollector;

import junit.framework.TestCase;

public class SemagrowMyTest extends TestCase {

	public void testSemagrowQuery() throws IOException {
	    
		String q1 = "" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q2 = "" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://www.opengis.net/ont/geosparql#hasGeometry> ?geom .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q3 = "" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://www.opengis.net/ont/geosparql#hasGeometry> ?geom9 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/1> <http://www.opengis.net/ont/geosparql#hasGeometry> ?geom1 .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q4 = "" +
				"SELECT * WHERE {\n" +
				"  ?id <http://www.opengis.net/ont/geosparql#hasGeometry> ?geom .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q5 = "" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt9 .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q6 = "" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/165506> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt_inv .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt9 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q7 = "" +
				"SELECT * WHERE {\n" +
				"  ?id <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		
		SemagrowSailFactory factory = new SemagrowSailFactory();
		SemagrowSailConfig config = new SemagrowSailConfig();
		Repository repo = new SemagrowSailRepository((SemagrowSail) factory.getSail(config));
		
		repo.initialize();
		        
		RepositoryConnection conn = repo.getConnection();
		
		TupleQuery query = conn.prepareTupleQuery(q7);
		
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