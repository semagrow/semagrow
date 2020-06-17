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
				"  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/245412> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt_inv .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt9 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q7 = "" +
				"SELECT * WHERE {\n" +
				"  ?id <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q8 = "" +
				"SELECT * WHERE {\n" +
				"  ?id <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"  ?id2 <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2 .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q9 = "" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"  ?id <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2 .\n" +
				// "  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
//		String q10 = "" +
//				"SELECT * WHERE {\n" +
//				"  ?id <http://www.opengis.net/ont/geosparql#asWKT> POINT(16.25613715 47.5043295) .\n" +
//				"}";
		
//		String q12 = "" +
//				"SELECT * WHERE {\n" +
//				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://www.opengis.net/ont/geosparql#hasGeometry> ?geom .\n" +
//				"  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
//				"}";
		
		String q11 = "" +
			"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
			"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
			"SELECT * WHERE {\n" +
			"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
			"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2 .\n" +
			"  FILTER(geof:distance(?wkt1,?wkt2,opengis:metre) < 10) .\n" +
			"}";
		
		String q12 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2 .\n" +
				"  BIND(geof:distance(?wkt1,?wkt2,opengis:metre) as ?dist) . \n" +
				"  FILTER(?dist < 10) .\n" +
				"}";
		
		String q13 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
				"  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/701155> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2 .\n" +
				"  BIND(geof:distance(?wkt1,?wkt2,opengis:metre) as ?dist) . \n" +
				"  FILTER(?dist < 10) .\n" +
//				"  FILTER(geof:distance(?wkt1,?wkt2,opengis:metre) < 10) .\n" +
				"}";
		
		String q14 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2 .\n" +
				"  BIND(geof:distance(?wkt1,?wkt2,opengis:metre) as ?dist) . \n" +
				"}";
		
		String q15 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1 .\n" +
				"  ?id <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2 .\n" +
//				"  FILTER(geof:distance(?wkt1,?wkt2,opengis:metre) < 10) .\n" +
				"  BIND(geof:distance(?wkt1,?wkt2,opengis:metre) as ?dist) . \n" +
				"  FILTER(?dist < 10) .\n" +
				"}";
		
		String q16 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://www.opengis.net/ont/geosparql#hasGeometry> ?geom .\n" +
				"  ?geom <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .\n" +
				"}";
		
		String q17 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://www.opengis.net/ont/geosparql#hasGeometry> ?l_geom_id .\n" + 
				"  <http://deg.iit.demokritos.gr/invekos/resource/701155> <http://www.opengis.net/ont/geosparql#hasGeometry> ?i_geom_id .\n" + 
				"  ?l_geom_id <http://www.opengis.net/ont/geosparql#asWKT> ?l_geom .\n" + 
				"  ?i_geom_id <http://www.opengis.net/ont/geosparql#asWKT> ?i_geom .\n" + 
				"}";
		
		String q18 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  <http://deg.iit.demokritos.gr/invekos/resource/701155> geo:hasGeometry ?i_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
//				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> geo:asWKT ?l_geom .\n" + 
//				"  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/701155> geo:asWKT ?i_geom .\n" + 
				"  FILTER(geof:distance(?l_geom,?i_geom,opengis:metre) < 10) .\n" +
				"}";
		
		String q19 = "" +		//query 1
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l_lc1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l_lc1_sp .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1> ?l_lc1 .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l_lc1_sp .\n" + 
				"  ?conversion <http://deg.iit.demokritos.gr/invekosCropTypeNumber> ?cropNu .\n" +
				"  ?i <http://deg.iit.demokritos.gr/invekos/hasCropTypeNumber> ?cropNu2 .\n" +
				"  FILTER(?cropNu = ?cropNu2) .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://www.opengis.net/ont/geosparql#hasGeometry> ?l_geom_id .\n" + 
				"  ?i <http://www.opengis.net/ont/geosparql#hasGeometry> ?i_geom_id .\n" + 
				"  ?l_geom_id <http://www.opengis.net/ont/geosparql#asWKT> ?l_geom .\n" + 
				"  ?i_geom_id <http://www.opengis.net/ont/geosparql#asWKT> ?i_geom .\n" + 
				"  FILTER(geof:distance(?l_geom,?i_geom,opengis:metre) < 10) .\n" +
				"}";
		
		String q20 = "" +		//a
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/9> geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  FILTER(geof:distance(?l_geom,?i_geom,opengis:metre) < 10) .\n" + 
				"}";
		
		String q21 = "" +		//a2
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"}";
		
		String q22 = "" +		//a2
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  FILTER(geof:distance(?l_geom,?i_geom,opengis:metre) < 10) .\n" + 
				"}";
		
		String q23 = "" +		//2 loukades ???
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  <http://deg.iit.demokritos.gr/lucas/resource/2> geo:hasGeometry ?l2_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?l2_geom_id geo:asWKT ?l2_geom .\n" + 
//				"  FILTER(geof:distance(?l_geom,?l2_geom,opengis:metre) < 10) .\n" + 
				"}";
		
		String q24 = "" +		//a2
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
//				"  FILTER(geof:distance(?l_geom,?i_geom,opengis:metre) < 10) .\n" + 
				"}";
		
		String q25 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  ?id geo:hasGeometry ?geom .\n" +
				"}";
		
		String q26 = "" +
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l_lc1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l_lc1_sp .\n" +
				"  ?id2 <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l2_lc1 .\n" +
				"  ?id2 <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l2_lc1_sp .\n" +
				"  FILTER(?l_lc1 = ?l2_lc1) .\n" +
				"  FILTER(?l_lc1_sp = ?l2_lc1_sp) .\n" +
				"}";
		
		String q27 = "" +			//query 2
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l_lc1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l_lc1_sp .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1> ?l_lc1 .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l_lc1_sp .\n" + 
				"  ?conversion <http://deg.iit.demokritos.gr/invekosCropTypeNumber> ?cropNu .\n" +
				"  ?i <http://deg.iit.demokritos.gr/invekos/hasCropTypeNumber> ?cropNu2 .\n" +
				"  FILTER(?cropNu = ?cropNu2) .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?i geo:hasGeometry ?i_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?dist) .\n" +
				"}\n" +
				"ORDER BY ASC(?dist)\n" +
                "LIMIT 1";
//				"}";
		
		String q28 = "" +		//query 5 ?
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l_lc1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l_lc1_sp .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1> ?l_lc1 .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l_lc1_sp .\n" + 
				"  ?conversion <http://deg.iit.demokritos.gr/invekosCropTypeNumber> ?cropNu .\n" +
				"  ?i <http://deg.iit.demokritos.gr/invekos/hasCropTypeNumber> ?cropNu2 .\n" +
				"  FILTER(?cropNu = ?cropNu2) .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?i geo:hasGeometry ?i_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?dist) .\n" +
				"  {\n" +
				"    SELECT DISTINCT ?dist2 WHERE {\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l2_lc1 .\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l2_lc1_sp .\n" +
				"      ?conversion2 <http://deg.iit.demokritos.gr/lucasLC1> ?l2_lc1 .\n" +
				"      ?conversion2 <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l2_lc1_sp .\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l2_geom_id .\n" +
				"      ?l2_geom_id geo:asWKT ?l2_geom .\n" +
				"      ?i2 geo:hasGeometry ?i2_geom_id .\n" +
				"      ?i2_geom_id geo:asWKT ?i2_geom .\n" +
				"      BIND(geof:distance(?l2_geom,?i2_geom,opengis:metre) as ?dist2) .\n" +
				"      FILTER(?dist2 < 10) .\n" +
				"    }\n" +
				"    ORDER BY ASC(?dist2)\n" +
				"    LIMIT 1\n" +
				"  }\n" +
				"  FILTER(?dist <= ?dist2) .\n" +
				"}";
		
		String q29 = "" +		//query 9 ?
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/1> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l_lc1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/1> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l_lc1_sp .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1> ?l_lc1 .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l_lc1_sp .\n" + 
				"  <http://deg.iit.demokritos.gr/lucas/resource/1> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?i <http://deg.iit.demokritos.gr/invekos/hasCropTypeNumber> ?nu .\n" +
				"  ?i geo:hasGeometry ?i_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?dist) .\n" +
                "  FILTER(?dist < 10) .\n" +
				"  {\n" +
				"    SELECT DISTINCT ?dist2 WHERE {\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/1> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l2_lc1 .\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/1> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l2_lc1_sp .\n" +
				"      ?conversion2 <http://deg.iit.demokritos.gr/lucasLC1> ?l2_lc1 .\n" +
				"      ?conversion2 <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l2_lc1_sp .\n" +
                "      ?conversion2 <http://deg.iit.demokritos.gr/invekosCropTypeNumber> ?cropNu .\n" +
				"      ?i2 <http://deg.iit.demokritos.gr/invekos/hasCropTypeNumber> ?cropNu2 .\n" +
				"      FILTER(?cropNu = ?cropNu2) .\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/1> geo:hasGeometry ?l2_geom_id .\n" +
				"      ?l2_geom_id geo:asWKT ?l2_geom .\n" +
				"      ?i2 geo:hasGeometry ?i2_geom_id .\n" +
				"      ?i2_geom_id geo:asWKT ?i2_geom .\n" +
				"      BIND(geof:distance(?l2_geom,?i2_geom,opengis:metre) as ?dist2) .\n" +
				"    }\n" +
				"    ORDER BY ASC(?dist2)\n" +
				"    LIMIT 1\n" +
				"  }\n" +
//				"  FILTER(?dist < ?dist2) .\n" +
				"}";

		String q30 = "" +		//query 10 ?
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/5> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l_lc1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/5> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l_lc1_sp .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1> ?l_lc1 .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l_lc1_sp .\n" + 
				"  <http://deg.iit.demokritos.gr/lucas/resource/5> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?i geo:hasGeometry ?i_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?dist) .\n" +
				"  {\n" +
				"    SELECT DISTINCT ?dist2 WHERE {\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/5> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l2_lc1 .\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/5> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l2_lc1_sp .\n" +
				"      ?conversion2 <http://deg.iit.demokritos.gr/lucasLC1> ?l2_lc1 .\n" +
				"      ?conversion2 <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l2_lc1_sp .\n" +
				"      <http://deg.iit.demokritos.gr/lucas/resource/5> geo:hasGeometry ?l2_geom_id .\n" +
				"      ?l2_geom_id geo:asWKT ?l2_geom .\n" +
				"      ?i2 geo:hasGeometry ?i2_geom_id .\n" +
				"      ?i2_geom_id geo:asWKT ?i2_geom .\n" +
				"      BIND(geof:distance(?l2_geom,?i2_geom,opengis:metre) as ?dist2) .\n" +
				"    }\n" +
				"    ORDER BY ASC(?dist2)\n" +
				"    LIMIT 1\n" +
				"  }\n" +
				"  FILTER(10 <= ?dist2) .\n" +
				"}\n" +
                "ORDER BY ASC(?dist)\n" +
                "LIMIT 1";
		
		String q31 = "" +			//query 2, without ordering and limit, and no invekos only lucases
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l_lc1 .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l_lc1_sp .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1> ?l_lc1 .\n" +
				"  ?conversion <http://deg.iit.demokritos.gr/lucasLC1_spec> ?l_lc1_sp .\n" + 
				"  ?i <http://deg.iit.demokritos.gr/lucas/hasLC1> ?l2_lc1 .\n" +
				"  ?i <http://deg.iit.demokritos.gr/lucas/hasLC1_SPEC> ?l2_lc1_sp .\n" +
				"  FILTER(?l_lc1 = ?l2_lc1) .\n" +
				"  FILTER(?l_lc1_sp = ?l2_lc1_sp) .\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/9> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?i geo:hasGeometry ?i_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?dist) .\n" +
//				"}\n" +
//				"ORDER BY ASC(?dist)\n" +
//                "LIMIT 1";
				"}";
		
		String q32 = "" +		//for query 9
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/1> geo:hasGeometry ?l_geom_id .\n" + 
				"  ?i <http://deg.iit.demokritos.gr/invekos/hasCropTypeNumber> ?nu .\n" +
				"  ?i geo:hasGeometry ?i_geom_id .\n" + 
				"  ?l_geom_id geo:asWKT ?l_geom .\n" + 
				"  ?i_geom_id geo:asWKT ?i_geom .\n" + 
				"  BIND(geof:distance(?l_geom,?i_geom,opengis:metre) as ?dist) .\n" +
                "  FILTER(?dist < 10) .\n" +
                "  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/1> geo:asWKT ?temp .\n" + 
                "  BIND(geof:distance(?l_geom,?temp,opengis:metre) as ?dist2) .\n" +
                "  FILTER(?dist < ?dist2) .\n" +
//				"  FILTER(?dist2 > 10) .\n" +
				"}";
		
		
		String q33 = "" +		//for query 9
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> geo:asWKT ?wkt1 .\n" + 
//                "  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/1> geo:asWKT ?wkt2 .\n" + 
                "  BIND(geof:distance(?wkt1,?wkt2,opengis:metre) as ?dist) .\n" +
				"  FILTER(?dist > 10) .\n" +
//				"  FILTER(10 < ?dist) .\n" +
				"}";
		
		String q34 = "" +		//for query 9
				"PREFIX geof: <http://www.opengis.net/def/function/geosparql/>" +
				"PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
				"PREFIX opengis: <http://www.opengis.net/def/uom/OGC/1.0/>" +
				"SELECT * WHERE {\n" +
				"  <http://deg.iit.demokritos.gr/lucas/resource/Geometry/1> geo:asWKT ?wkt1 .\n" + 
				"  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/245412> geo:asWKT ?wkt2 .\n" + 
				"  <http://deg.iit.demokritos.gr/invekos/resource/Geometry/1> geo:asWKT ?wkt3 .\n" + 
				"  BIND(geof:distance(?wkt1,?wkt2,opengis:metre) as ?dist) .\n" +
                "  BIND(geof:distance(?wkt1,?wkt3,opengis:metre) as ?dist2) .\n" +
                "  FILTER(?dist < 10) .\n" +
                "  FILTER(?dist < ?dist2) .\n" +
//				"  FILTER(?dist2 > 10) .\n" +
				"}";
		
		
		
		SemagrowSailFactory factory = new SemagrowSailFactory();
		SemagrowSailConfig config = new SemagrowSailConfig();
		Repository repo = new SemagrowSailRepository((SemagrowSail) factory.getSail(config));
		
		repo.initialize();
		        
		RepositoryConnection conn = repo.getConnection();
		
		// lucas gids: 1, 2, 4, 5, 7, 9, 10 (total: 7)
		// invekos gids: 701155, 245412, 1, 2, 3, 4, 5, 6, 7 (total: 9)
		// * = total_lucas_geom + total_invekos_geom
		// $ = total_lucas + total_invekos
		// q1: 1, q2: 1, q3: 1, q4: $, q5: 1, q6: 1, q7: *, q8: error, q9: *, q11: 0, q12: 0, q13: 1 (+dist)
		// q14: 1, q15: 2, q16: 1, q17: 1, q18: 1, q19: 1, q20: 2, q21: 1, q22: 2, q23: 1, q24: *, q25: *, q26: 2
		// q27: 1 (without order by + limit: 27 wrong)
		// q28: 0 wrong
		// q29: More than one triples with two free variables, wrong
		// q30: 0 wrong
		// q31: 5 ??? wrong
		TupleQuery query = conn.prepareTupleQuery(q34);
		
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