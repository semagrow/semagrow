package org.semagrow.connector.postgis.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostGISQueryStringUtil {
	
//	private static Map<String, String> wktMap = new HashMap<String, String>();
//	private static Map<String, String> typeMap = new HashMap<String, String>();
	
	private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);
	private static String TYPE_URI;
	private static String INDEX_BINDING_NAME = ".id";
	private static String WKT_BINDING_NAME = ".wkt";
	private static String DEFAULT_TABLE_NAME = "geometries";
	private static String SRID = ", 4326)";
	private static String ST_ASTEXT = "ST_AsText(";
	private static String ST_EQUALS = "ST_Equals(";
	private static String ST_DISTANCE = "ST_Distance(";
	private static String ST_GEOM_FROM_TEXT = "ST_GeomFromText(";
	private static String ST_GEOGR_FROM_TEXT = "ST_GeographyFromText(";	
	private static String SELECT = "SELECT ";
	private static String FROM = " FROM ";
	private static String WHERE = " WHERE ";
	private static String UNION = " UNION ";
	private static String COMMA_SEP = ", ";
	private static String AND_SEP = " AND ";
	private static String AS = " AS ";
	private static String G = "table";
	
	private static void typeURI(String dbname) {
		TYPE_URI = "http://rdf.semagrow.org/pgm/" + dbname + "/geometry";
	}
	
	private static String geomFromText(String a, String b) {
		return "ST_GeomFromText(" + a + ", " + SRID + ")";
	}
	
	public static void createTripleMaps(List<String> triples, Map<String, String> wktMap, Map<String, String> typeMap) {
		int i = 0;
		while (i < triples.size()) {
			if (triples.get(i+1).contains("#asWKT")) {
				wktMap.put(triples.get(i), triples.get(i+2));
			}
			else if (triples.get(i+1).contains("#type")) {
				typeMap.put(triples.get(i), triples.get(i+2));
			}
			i += 3;
		}
		
		logger.debug("wktMap: {}", wktMap);
		logger.debug("typeMap: {}", typeMap);
	}
	
	public static String buildSimpleSQLQuery(Set<String> freeVars, Map<String,String> bindingVars, 
			Map<String,String> extraBindingVars, 
			Map<String, String> wktMap, Map<String, String> typeMap, 
			Set<String> selectSet, Set<String> fromSet, Set<String> whereSet) {
//		Set<String> selectList = new HashSet<String>();
//		Set<String> fromList = new HashSet<String>();
//		Set<String> whereList = new HashSet<String>();
		int i = 0;
		
		for (Entry<String, String> wkts : wktMap.entrySet()) {
			if (freeVars.contains(wkts.getKey())) {
				selectSet.add(G + i + INDEX_BINDING_NAME + AS + wkts.getKey());
				bindingVars.put(wkts.getKey(), G + i + INDEX_BINDING_NAME);
			}
			else {
				whereSet.add(G + i + INDEX_BINDING_NAME + " = " + decomposeToId(wkts.getKey()));
			}
			if (freeVars.contains(wkts.getValue())) {
				selectSet.add(ST_ASTEXT + G + i + WKT_BINDING_NAME + ")" + AS + wkts.getValue());
				bindingVars.put(wkts.getValue(), G + i + WKT_BINDING_NAME);
			}
			else {
				if (!freeVars.contains(wkts.getKey())) selectSet.add(G + i + INDEX_BINDING_NAME);
				whereSet.add(ST_EQUALS + G + i + WKT_BINDING_NAME + COMMA_SEP + ST_GEOM_FROM_TEXT + wkts.getValue().replace("\"", "\'") + SRID + ")");
			}
			fromSet.add(DEFAULT_TABLE_NAME + " " + G + i);
			i++;
		}
		
		for (Entry<String, String> types : typeMap.entrySet()) {			
			if (wktMap.containsKey(types.getKey())) 
				logger.debug("same key!!!: {}", types.getKey());
			else {
				if (freeVars.contains(types.getKey())) {
					selectSet.add(G + i + INDEX_BINDING_NAME + AS + types.getKey());
					bindingVars.put(types.getKey(), G + i + INDEX_BINDING_NAME);
				}
				else {
					selectSet.add(G + i + INDEX_BINDING_NAME);
					whereSet.add(G + i + INDEX_BINDING_NAME + " = " + decomposeToId(types.getKey()));
				}
				fromSet.add(DEFAULT_TABLE_NAME + " " + G + i);
			}
			
			if (freeVars.contains(types.getValue())) {
				extraBindingVars.put(types.getValue(), TYPE_URI);
			}
			else {
				if (types.getValue().equals(TYPE_URI))
					logger.debug("correct type uri");
				else 
					logger.debug("wrong type uri - return NULL"); //return NULL ???
			}
			i++;
		}
		String query = serializeSet(selectSet, COMMA_SEP, SELECT) 
				+ serializeSet(fromSet, COMMA_SEP, FROM)
				+ serializeSet(whereSet, AND_SEP, WHERE);
		
		logger.debug("bindingVars (simple sql query): {}", bindingVars);
		logger.debug("query (simple sql query): {}", query);
		
		return query;
	}
	
	
	public static String buildSQLQuery(TupleExpr expr, Set<String> freeVars, List<String> tables, 
			BindingSet bindings, Map<String,String> extraBindingVars, String dbname) {
		
		logger.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! buildSQLQuery");
		
		typeURI(dbname);
		List<String> triples = computeTriples(expr);
//		List<String> filterInfo = computeFilterVars(expr);
//		List<String> bindInfo = computeBindVars(expr);
			
		// remove all triples with predicate different than #asWKT
		keepWKTAndTypePredicates(triples);
		if (triples.isEmpty()) return null;
		
		logger.debug("triples: {}", triples);
//		logger.debug("filter variables: {} ", filterInfo);
//		logger.debug("bind variables: {} ", bindInfo);
		
		Map<String,String> bindingVars = new HashMap<String, String>();
		
		Set<String> bindingNames = bindings.getBindingNames();
		for (Object binding: bindingNames) {
			logger.debug("bindings: {} - {}", binding, bindings.getValue((String)binding));
			while (triples.contains(binding.toString())) {
				int index = triples.indexOf(binding);
				triples.remove(index);
				triples.add(index, bindings.getValue((String)binding).toString());
			}
			bindingVars.put((String) binding, bindings.getValue((String)binding).toString().replace("\"", "\'"));
		}
		
		logger.debug("triples 2: {}", triples.toString());
		logger.debug("bindingVars 2: {}", bindingVars);
		
		
		String var1, var2, operator;
		Set<List<String>> allFilters = computeFilterVars(expr);
		for (List<String> filterInfo : allFilters) {
			if (filterInfo.size() == 3) {
				operator = filterInfo.get(0);
				var1 = filterInfo.get(1);
				var2 = filterInfo.get(2);
				if ((freeVars.contains(var1) && freeVars.contains(var2)) || freeVars.contains(var2)) {
					while (triples.contains(var2)) {
						if (operator.equals("=")) {
							int index = triples.indexOf(var2);
							triples.remove(index);
							triples.add(index, var1);
						}
					}
					if (!bindingVars.get(var1).isEmpty() && !bindingVars.get(var2).isEmpty()) 
						logger.debug("what happens if bindingVars contains both variables?");
				}
				else if (freeVars.contains(var1)) {
					while (triples.contains(var1)) {
						if (operator.equals("=")) {
							int index = triples.indexOf(var1);
							triples.remove(index);
							triples.add(index, var2);
						}
					}
					bindingVars.remove(bindingVars.remove(var1));
				}
				else {
					logger.debug("what happens if both are constant values");
				}
			}
		}
		
		logger.debug("triples 3: {}", triples.toString());
		logger.debug("bindingVars 3: {}", bindingVars);
		
		
		
		
		
		Map<String, String> wktMap = new HashMap<String, String>();
		Map<String, String> typeMap = new HashMap<String, String>();
		createTripleMaps(triples, wktMap, typeMap);
		
		Set<String> select = new HashSet<String>();
		Set<String> from = new HashSet<String>();
		Set<String> where = new HashSet<String>();
//		String query = buildSimpleSQLQuery(freeVars, bindingVars, extraBindingVars, 
//				wktMap, typeMap, select, from, where);
		buildSimpleSQLQuery(freeVars, bindingVars, extraBindingVars, 
				wktMap, typeMap, select, from, where);
		
		
		
		String call, unit, value, condition = "";
		for (List<String> filterInfo : allFilters) {
			if (filterInfo.size() == 6) {
				operator = filterInfo.get(0);
				call = filterInfo.get(1);
				var1 = filterInfo.get(2);
				var2 = filterInfo.get(3);
				unit = filterInfo.get(4);
				value = filterInfo.get(5);
				if (call.equals("distance")) condition += ST_DISTANCE;
				if (unit.equals("metre")) condition += ST_GEOGR_FROM_TEXT + ST_ASTEXT;
				else if (unit.equals("degree")) condition += ST_GEOM_FROM_TEXT + ST_ASTEXT;
				if (bindingVars.containsKey(var1)) {
					var1 = bindingVars.get(var1);
					if (var1.contains("^")) var1 = var1.substring(1, var1.indexOf("^"));
					logger.debug("--- bindingVars: {}", var1);
					condition += var1;
				}
				else {		// IS THIS CORRECT ???
					logger.debug("ELSE!!!!! ");
					bindingVars.put(var1, G + (triples.size()+1) + WKT_BINDING_NAME);
					condition += bindingVars.get(var1);
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (unit.equals("metre")) condition += ")), " + ST_GEOGR_FROM_TEXT + ST_ASTEXT;
				else if (unit.equals("degree")) condition += ")" + SRID + COMMA_SEP + ST_GEOM_FROM_TEXT + ST_ASTEXT;
				if (bindingVars.containsKey(var2)) {
					logger.debug("filterInfo : {} ", filterInfo);
					var2 = bindingVars.get(var2);
					if (var2.contains("^")) var2 = var2.substring(1, var2.indexOf("^"));
					logger.debug("--- bindingVars: {}", var2);
					condition += var2;
				}
				else {		// IS THIS CORRECT ???
					logger.debug("ELSE!!!!! ");
					bindingVars.put(var2, G + (triples.size()+1) + WKT_BINDING_NAME);
					condition += bindingVars.get(var2);
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (unit.equals("metre")) condition += "))";
				else if (unit.equals("degree")) condition += ")" + SRID;
				condition += ") ";
				condition += operator + " " + value;
				logger.debug("!!!! condition : {}", condition); 
				where.add(condition);
			}
		}
		
		
		String sqlQuery = serializeSet(select, COMMA_SEP, SELECT) 
				+ serializeSet(from, COMMA_SEP, FROM)
				+ serializeSet(where, AND_SEP, WHERE) + ";";
		
		logger.debug("!!!!! mpla : {} ", sqlQuery);
		
		return sqlQuery;
	}
	
	
public static String buildSQLQueryUnion(TupleExpr expr, Set<String> freeVars, 
		List<String> tables, List<BindingSet> bindings, Collection<String> relevantBindingNames, 
		Map<String,String> extraBindingVars, String dbname) {

		
		logger.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! buildSQLQueryUNION !!!!!!!!!!!!!!!!!!");
		
		List<String> triples = computeTriples(expr);
//		List<String> bindInfo = computeBindVars(expr);
		
		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
			logger.error("More than one triples with two free variables.");
			throw new QueryEvaluationException();
		}
		
		keepWKTAndTypePredicates(triples);
		if (triples.isEmpty()) return null;
		
		logger.debug("triples: {}", triples);
		
		Map<String,String> bindingVars = new HashMap<String, String>();

//		Set<String> bindingNames;
//		for (BindingSet binding: bindings)
//			bindingNames = binding.getBindingNames();
//		
//		for (Object binding: bindingNames) {
//			logger.debug("bindings: {} - {}", binding, bindings.getValue((String)binding));
//			while (triples.contains(binding.toString())) {
//				int index = triples.indexOf(binding);
//				triples.remove(index);
//				triples.add(index, bindings.getValue((String)binding).toString());
//			}
//			bindingVars.put((String) binding, bindings.getValue((String)binding).toString().replace("\"", "\'"));
//		}
		
		logger.debug("triples 2: {}", triples.toString());
		logger.debug("bindingVars 2: {}", bindingVars);
		logger.debug("relevantBindingNames: {}", relevantBindingNames);
		
		
		String var1, var2, operator;
		Set<List<String>> allFilters = computeFilterVars(expr);
		for (List<String> filterInfo : allFilters) {
			if (filterInfo.size() == 3) {
				operator = filterInfo.get(0);
				var1 = filterInfo.get(1);
				var2 = filterInfo.get(2);
				if ((freeVars.contains(var1) && freeVars.contains(var2)) || freeVars.contains(var2)) {
					while (triples.contains(var2)) {
						if (operator.equals("=")) {
							int index = triples.indexOf(var2);
							triples.remove(index);
							triples.add(index, var1);
						}
					}
					if (!bindingVars.get(var1).isEmpty() && !bindingVars.get(var2).isEmpty()) 
						logger.debug("what happens if bindingVars contains both variables?");
				}
				else if (freeVars.contains(var1)) {
					while (triples.contains(var1)) {
						if (operator.equals("=")) {
							int index = triples.indexOf(var1);
							triples.remove(index);
							triples.add(index, var2);
						}
					}
					bindingVars.remove(bindingVars.remove(var1));
				}
				else {
					logger.debug("what happens if both are constant values");
				}
			}
		}
		
		logger.debug("triples 3: {}", triples.toString());
		logger.debug("bindingVars 3: {}", bindingVars);
		
		
		
		
		
		Map<String, String> wktMap = new HashMap<String, String>();
		Map<String, String> typeMap = new HashMap<String, String>();
		createTripleMaps(triples, wktMap, typeMap);
		
		Set<String> select = new HashSet<String>();
		Set<String> from = new HashSet<String>();
		Set<String> where = new HashSet<String>();
//		String query = buildSimpleSQLQuery(freeVars, bindingVars, extraBindingVars, 
//				wktMap, typeMap, select, from, where);
		buildSimpleSQLQuery(freeVars, bindingVars, extraBindingVars, 
				wktMap, typeMap, select, from, where);
		
		
		
		String call, unit, value, condition = "";
		boolean containBindings;
		for (List<String> filterInfo : allFilters) {
			if (filterInfo.size() == 6) {
				containBindings = false;
				operator = filterInfo.get(0);
				call = filterInfo.get(1);
				var1 = filterInfo.get(2);
				var2 = filterInfo.get(3);
				unit = filterInfo.get(4);
				value = filterInfo.get(5);
				if (call.equals("distance")) condition += ST_DISTANCE;
				if (unit.equals("metre")) condition += ST_GEOGR_FROM_TEXT + ST_ASTEXT;
				else if (unit.equals("degree")) condition += ST_GEOM_FROM_TEXT + ST_ASTEXT;
				if (bindingVars.containsKey(var1)) {
					var1 = bindingVars.get(var1);
					if (var1.contains("^")) var1 = var1.substring(1, var1.indexOf("^"));
					logger.debug("--- bindingVars: {}", var1);
					condition += var1;
				}
				else {		// IS THIS CORRECT ???
					logger.debug("ELSE!!!!! ");
					containBindings = true;
					condition += var1;
//					bindingVars.put(var1, G + (triples.size()+1) + WKT_BINDING_NAME);
//					condition += bindingVars.get(var1);
//					triples.add(null); triples.add(null); triples.add(null);
				}
				if (unit.equals("metre")) condition += ")), " + ST_GEOGR_FROM_TEXT + ST_ASTEXT;
				else if (unit.equals("degree")) condition += ")" + SRID + COMMA_SEP + ST_GEOM_FROM_TEXT + ST_ASTEXT;
				if (bindingVars.containsKey(var2)) {
					logger.debug("filterInfo : {} ", filterInfo);
					var2 = bindingVars.get(var2);
					if (var2.contains("^")) var2 = var2.substring(1, var2.indexOf("^"));
					logger.debug("--- bindingVars: {}", var2);
					condition += var2;
				}
				else {		// IS THIS CORRECT ???
					logger.debug("ELSE!!!!! ");
					containBindings = true;
					condition += var2;
//					bindingVars.put(var2, G + (triples.size()+1) + WKT_BINDING_NAME);
//					condition += bindingVars.get(var2);
//					triples.add(null); triples.add(null); triples.add(null);
				}
				if (unit.equals("metre")) condition += "))";
				else if (unit.equals("degree")) condition += ")" + SRID;
				condition += ") ";
				condition += operator + " " + value;
				logger.debug("!!!! condition : {}", condition); 
//				if (!containBindings)
					where.add(condition);
			}
		}
		
		logger.debug("after filter: bindingVars : {}", bindingVars);
		
		String sqlQuery = serializeSet(select, COMMA_SEP, SELECT) 
				+ serializeSet(from, COMMA_SEP, FROM)
				+ serializeSet(where, AND_SEP, WHERE);
		
		String union = "", temp = "";
		for (String relevantBindingName: relevantBindingNames) {
			if (sqlQuery.contains(relevantBindingName)) {
				for (BindingSet binding: bindings) {
					temp += binding.getValue(relevantBindingName);
					if (temp.contains("^")) {
						temp = temp.substring(1, temp.indexOf("^"));
						temp = temp.replace("\"", "'");
						union += sqlQuery.replace(relevantBindingName, temp) + UNION;
						temp = "";
					}
				}
			}
//			union += relevantBindingName;
//			union += " IN (";
//			for (BindingSet binding: bindings) {
//				temp += binding.getValue(relevantBindingName);
//				if (temp.contains("^")) {
//					temp = temp.substring(1, temp.indexOf("^"));
//					union += ST_GEOGR_FROM_TEXT + ST_ASTEXT + temp.replace("\"", "'") + "))";
//				}
//				else 
//					union += temp;
//				union += ", ";
//				temp = "";
//			}
//			union = union.substring(0, union.length() - 2);
//			union += ")";
//			where.add(union);
//			union = "";
		}
		union = union.substring(0, union.length() - 6);
		
		logger.debug("!!!!! sqlQuery : {} ", sqlQuery);
		logger.debug("!!!!! union : {} ", union);
		
		return union + ";";
	}
	
	
//	public static String buildSQLQueryUnion(TupleExpr expr, Set<String> freeVars, List<String> tables, List<BindingSet> bindings, Collection<String> relevantBindingNames) {
//		
//		logger.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! buildSQLQueryUNION !!!!!!!!!!!!!!!!!!");
//		
//		List<String> triples = computeTriples(expr);
//		Set<List<String>> filterInfo = computeFilterVars(expr);
//		List<String> bindInfo = computeBindVars(expr);
//		
//		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
//			logger.error("More than one triples with two free variables.");
//			throw new QueryEvaluationException();
//		}
//		
//		keepWKTAndTypePredicates(triples);
//		if (triples.isEmpty()) return null;
//		
//		logger.debug("triples: {}", triples);
//		logger.debug("filter variables: {} " ,filterInfo);
//		logger.debug("bind variables: {} " ,bindInfo);
//		
//		Map<String,String> bindingVars = new HashMap<String, String>();
//		
////		logger.debug("filterVars: {}", filterVars.toString());
//		
////		Set<String> bindingNames = bindings.getBindingNames();
////		for (Object binding: bindingNames) {
////			logger.debug("bindings: {} - {}", binding, bindings.getValue((String)binding));
////			if (triples.contains(binding.toString())) {
////				int index = triples.indexOf(binding);
////				triples.remove(index);
////				triples.add(index, bindings.getValue((String)binding).toString());
////			}
//////			asToVar.put((String) binding, bindings.getValue((String)binding).toString().replace("\"", "\'"));
////		}
//		
//		Pair<String, String> tableAndΙd = null;
//		List<String> select = new ArrayList<String>();
//		List<String> from = new ArrayList<String>();
//		List<String> where = new ArrayList<String>();
//		String wkt = "", binding_var = "";
//		int binding_place = -1;
//		int place = 0;
//		
//		for (String part : triples) {
//			place++;
//			if (part.contains("#")) {	// predicate
//				wkt = "t" + (place-1) + WKT_BINDING_NAME;
//			}
//			else {			
//				if (place % 3 != 0) {	// subject
//					if (freeVars.contains(part)) {	// free var as subject (from ?)
//						select.add("t"+ place + INDEX_BINDING_NAME + " AS " + part);
//						bindingVars.put(part, "t"+ place + INDEX_BINDING_NAME);
//						tables.add("?");
//					}
//					else if (bindings.get(0).hasBinding(part)) {
//						tableAndΙd = subjectDecompose(bindings.get(0).getBinding(part).getValue().toString());
//						from.add(tableAndΙd.getLeft() + " t" + place);
//						binding_var = part;
//						binding_place = place;
//					}
//					else {
//						tableAndΙd = subjectDecompose(part);
//						from.add(tableAndΙd.getLeft() + " t" + place);
//						where.add("t" + place + INDEX_BINDING_NAME + " = " + tableAndΙd.getRight());
//					}
//				}
//				else {					// object
//					if (freeVars.contains(part)) {
//						select.add(ST_ASTEXT + wkt + ") AS " + part);
//						bindingVars.put(part, wkt);
//						tables.add("-");
//					}
//					else {
//						where.add("ST_Equals(t" + place + WKT_BINDING_NAME + COMMA_SEP + ST_GEOM_FROM_TEXT + "'" + part + "'" + SRID + ")");
//						if (tables.get(place/3 - 1).equals("?"))
//							tables.remove(place/3 - 1);
//						if (part.contains("POINT")) {
//							tables.add("lucas");
//						}
//						else if (part.contains("MULTIPOLYGON")) {
//							tables.add("invekos");
//						}
//						else if (part.contains("POLYGON")) {
//							tables.add(DEFAULT_TABLE_NAME);
//						}
//					}
//					
//				}
//			}
//		}
//		
//		logger.debug("select:: {}", select);
//		logger.debug("from:: {}", from);
//		logger.debug("where:: {}", where);
//		logger.debug("tables:: {}", tables.toString());
//		
//		
//		String sqlQuery = "";
////		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
////			logger.error("More than one triples with two free variables.");
////			throw new QueryEvaluationException();
////		}
//		
//		Set<String> binds = new HashSet<String>();
//		List<String> filters = new ArrayList<String>();
//		
//		computeBindsAndFilters(expr, triples, binds, filters, tables, bindingVars);
//		
//		logger.debug("binds:: {}", binds);
//		logger.debug("filters:: {}", filters);
//		
//		for (String b : binds)
//			select.add(b);
//		
//		for (String f : filters)
//			where.add(f);
//		
//		
//		if (from.isEmpty()) {
//			if (ONE_TABLE) {
//				sqlQuery = serializeList(select, COMMA_SEP, SELECT) 
//						+ " FROM geometries t1" 
//						+ serializeList(where, AND_SEP, WHERE) + ";";
//			}
//			else {
//				String from1 = "", from2 = "", from3 = "", from4 = "";
//				logger.debug("vars size: {}", freeVars.size());
//				logger.debug("triples size: {}", triples.size());
//				logger.debug("asToVar size: {}", bindingVars.size());
//				if (triples.size() == 3) {
//					from1 = " FROM lucas t1";
//					from2 = " FROM invekos t1";
//					sqlQuery = serializeList(select, COMMA_SEP, SELECT) + from1 
//							+ serializeList(where, AND_SEP, WHERE) + UNION 
//							+ serializeList(select, COMMA_SEP, SELECT) + from2
//							+ serializeList(where, AND_SEP, WHERE) + ";";
//				}
//				else if (triples.size() == 6) {
//					from1 = " FROM lucas t1, lucas t4";
//					from2 = " FROM lucas t1, invekos t4";
//					from3 = " FROM invekos t1, lucas t4";
//					from4 = " FROM invekos t1, invekos t4";
//					sqlQuery = serializeList(select, COMMA_SEP, SELECT) + from1 
//							+ serializeList(where, AND_SEP, WHERE) + UNION 
//							+ serializeList(select, COMMA_SEP, SELECT) + from2
//							+ serializeList(where, AND_SEP, WHERE) + UNION 
//							+ serializeList(select, COMMA_SEP, SELECT) + from3
//							+ serializeList(where, AND_SEP, WHERE) + UNION 
//							+ serializeList(select, COMMA_SEP, SELECT) + from4
//							+ serializeList(where, AND_SEP, WHERE) + ";";
//				}
//			}
//		}
//		else {
//			if (ONE_TABLE) {
//				logger.debug("WHAT TO DO HERE ?????");
//			}
//			else {
//				logger.debug("vars size: {}", freeVars.size());
//				logger.debug("triples size: {}", triples.size());
//				
//				String where_bind = "";
//				
//				logger.debug("now sqlQuery: {}", sqlQuery);
//				
//				for (BindingSet b : bindings) {
//					if (!sqlQuery.equals(""))
//						sqlQuery += UNION;
//					
//					tableAndΙd = subjectDecompose(b.getBinding(binding_var).getValue().toString());
//					where_bind = "t" + binding_place + INDEX_BINDING_NAME + " = " + tableAndΙd.getRight();	
//					
//					if (freeVars.size() == triples.size() / 3 && !triples.contains(null)) {
//						sqlQuery += serializeList(select, COMMA_SEP, SELECT) 
//								+ serializeList(from, COMMA_SEP, FROM)
//								+ serializeList(where, AND_SEP, WHERE);
//						if (where.isEmpty()) sqlQuery += WHERE;
//						else sqlQuery += AND_SEP;
//						sqlQuery += where_bind;
//					}
//					else {
//						String lucas = "", invekos = "";
//						place = 1;
//						while (place < triples.size()) {
//							if (!from.contains("lucas t" + place) && !from.contains("invekos t" + place)) {
//								lucas += ", lucas t" + place;
//								invekos += ", invekos t" + place;
//							}
//							place += 3;
//						}
//						sqlQuery += serializeList(select, COMMA_SEP, SELECT) 
//								+ serializeList(from, COMMA_SEP, FROM) + lucas
//								+ serializeList(where, AND_SEP, WHERE);
//						if (where.isEmpty()) sqlQuery += WHERE;
//						else sqlQuery += AND_SEP;
//						sqlQuery += where_bind + UNION;
//						sqlQuery += serializeList(select, COMMA_SEP, SELECT) 
//								+ serializeList(from, COMMA_SEP, FROM) + invekos
//								+ serializeList(where, AND_SEP, WHERE);
//						if (where.isEmpty()) sqlQuery += WHERE;
//						else sqlQuery += AND_SEP;
//						sqlQuery += where_bind;
//					}
//				}
//				
//				sqlQuery += ";";
//			}
//		}
//		
//		logger.debug("bindingVars:: {}", bindingVars.toString());
//		logger.debug("sqlQuery:: {}", sqlQuery);
//		return sqlQuery;
//	}
	
	
	protected static void computeBindsAndFilters(TupleExpr expr, List<String> triples, Set<String> binds, List<String> filters, List<String> tables, Map<String,String> bindingVars) {
		Set<List<String>> allFilters = computeFilterVars(expr);
		List<String> bindInfo = computeBindVars(expr);
		
		logger.debug("filter variables: {} ", allFilters);
		logger.debug("bind variables: {} ", bindInfo);
		
		String dist = "";
		String compare = "", function = "", type = "";
		String condition = "";
		int i = 0;
		for (List<String> filterInfo : allFilters) {
			if (filterInfo.size() == 3) {
				logger.debug("before bindingVars: {}", bindingVars);
				bindingVars.replace(filterInfo.get(1), filterInfo.get(2));
				logger.debug("after bindingVars: {}", bindingVars);
				continue;
			}
			if (filterInfo.size() == 6) {
				String operator = filterInfo.get(0);
				String call = filterInfo.get(1);
				String var1 = filterInfo.get(2);
				String var2 = filterInfo.get(3);
				String unit = filterInfo.get(4);
				String value = filterInfo.get(5);
				if (call.equals("distance")) condition += ST_DISTANCE;
				if (unit.equals("metre")) condition += ST_GEOGR_FROM_TEXT + ST_ASTEXT;
				else if (unit.equals("degree")) condition += ST_GEOM_FROM_TEXT + ST_ASTEXT;
				if (bindingVars.containsKey(var1)) {
					var1 = bindingVars.get(var1);
					if (var1.contains("^")) var1 = var1.substring(1, var1.indexOf("^"));
					logger.debug("--- bindingVars: {}", var1);
					condition += var1;
				}
				else {
					bindingVars.put(var1, G + (triples.size()+1) + WKT_BINDING_NAME);
					condition += bindingVars.get(var1);
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (unit.equals("metre")) condition += ")), " + ST_GEOGR_FROM_TEXT + ST_ASTEXT;
				else if (unit.equals("degree")) condition += ")" + SRID + COMMA_SEP + ST_GEOM_FROM_TEXT + ST_ASTEXT;
				if (bindingVars.containsKey(var2)) {
					logger.debug("filterInfo : {} ", filterInfo);
					var2 = bindingVars.get(var2);
					if (var2.contains("^")) var2 = var2.substring(1, var2.indexOf("^"));
					logger.debug("--- bindingVars: {}", var2);
					condition += var2;
				}
				else {
					bindingVars.put(var2, G + (triples.size()+1) + WKT_BINDING_NAME);
					condition += bindingVars.get(var2);
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (unit.equals("metre")) condition += "))";
				else if (unit.equals("degree")) condition += ")" + SRID;
				condition += ") ";
				condition += operator + " " + value;
				logger.debug("!!!! dist : {}", condition); 
			}
			
			
			
			while (i < filterInfo.size()) {
				if (filterInfo.get(i).matches("[0-9]+")) {
					logger.debug("filterInfo.get(i): {}", filterInfo.get(i));
					dist += filterInfo.get(i);
					i++;
					if (!dist.contains(compare)) {
	//					binds.add(dist + "AS " + bv.get(b-1));
						dist += " " + compare + " ";
					}
					else {
						filters.add(dist);
						dist = "";
					}
				}
				else if (filterInfo.get(i).matches("[!<=>]+")) {
					compare = filterInfo.get(i);
					i++;
				}
				else {
					logger.debug("filterInfo: {} ", filterInfo);
					function = filterInfo.get(i);
					if (function.equals("distance")) dist += ST_DISTANCE;
					type = filterInfo.get(i+3);
					if (type.equals("metre")) dist += ST_GEOGR_FROM_TEXT + ST_ASTEXT;
					else if (type.equals("degree")) dist += ST_GEOM_FROM_TEXT + ST_ASTEXT;
					if (bindingVars.containsKey(filterInfo.get(i+1))) {
						String var = bindingVars.get(filterInfo.get(i+1));
						if (var.contains("^")) var = var.substring(1, var.indexOf("^"));
						logger.debug("--- bindingVars: {}", var);
						dist += var;
					}
					else {
						bindingVars.put(filterInfo.get(i+1), "t" + (triples.size()+1) + WKT_BINDING_NAME);
						dist += bindingVars.get(filterInfo.get(i+1));
						triples.add(null); triples.add(null); triples.add(null);
					}
					logger.debug("--- dist: {}", dist);
					if (type.equals("metre")) dist += ")), " + ST_GEOGR_FROM_TEXT + ST_ASTEXT;
					else if (type.equals("degree")) dist += ")" + SRID + COMMA_SEP + ST_GEOM_FROM_TEXT + ST_ASTEXT;
					if (bindingVars.containsKey(filterInfo.get(i+2))) {
						logger.debug("filterInfo : {} ", filterInfo);
						String var = bindingVars.get(filterInfo.get(i+2));
						if (var.contains("^")) var = var.substring(0, var.indexOf("^"));
						logger.debug("--- bindingVars: {}", var);
						dist += var;
					}
					else {
						bindingVars.put(filterInfo.get(i+2), "t" + (triples.size()+1) + WKT_BINDING_NAME);
						dist += bindingVars.get(filterInfo.get(i+2));
						triples.add(null); triples.add(null); triples.add(null);
					}
					if (type.equals("metre")) dist += "))";
					else if (type.equals("degree")) dist += ")" + SRID;
					dist += ") ";
					
					// Gathering binds
					for (int b = 0; b < bindInfo.size(); b += 5) {
						if (filterInfo.get(i).equals(bindInfo.get(b+1)) && filterInfo.get(i+1).equals(bindInfo.get(b+2)) && filterInfo.get(i+2).equals(bindInfo.get(b+3)) && filterInfo.get(i+3).equals(bindInfo.get(b+4))) {
							if (!dist.contains(compare)) binds.add(dist + "AS " + bindInfo.get(b));
							else binds.add(dist.substring(dist.lastIndexOf(compare) + 2) + "AS " + bindInfo.get(b));
							break;
						}
					}
					
					// Gathering filters
					if (!dist.contains(compare)) {
						dist += compare + " ";
					}
					else {
						filters.add(dist);
						dist = "";
					}
					
					i += 4;
				}
			}
		}
	}
	
	
	
	protected static List<String> computeBindVars(TupleExpr serviceExpression) {
		final List<String> res = new ArrayList<String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(ExtensionElem node) throws RuntimeException {
				// take all info about bind nodes, i.e. binding name, vars, values etc
				String signature = node.getSignature();
				res.add(signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));

				node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
					
					@Override
					public void meet(FunctionCall node) throws RuntimeException {
						String function = node.getSignature();
						res.add(function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
						
						node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
							@Override
							public void meet(Var node) throws RuntimeException {
								res.add(node.getName());
							}
							
							@Override
							public void meet(ValueConstant node) throws RuntimeException {
								String type = node.getValue().toString();
								res.add(type.substring(type.lastIndexOf("/") + 1));
							}
						});
					}
				});
			}
		});
		return res;
	}
	
	
	protected static Set<List<String>> computeFilterVars(TupleExpr serviceExpression) {
		final Set<List<String>> allRes = new HashSet<List<String>>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(Compare node) throws RuntimeException {
				final List<String> res = new ArrayList<String>();
				// take all info about compare nodes, i.e. operation, vars, values etc
				String signature = node.getSignature();
				res.add(signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));
				logger.debug("--- signature: {}", signature);
				
				node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
					
					@Override
					public void meet(FunctionCall node) throws RuntimeException {
						String function = node.getSignature();
						res.add(function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
						logger.debug("--- function: {}", function);
						
						node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
							@Override
							public void meet(Var node) throws RuntimeException {
								res.add(node.getName());
								logger.debug("--- CHILD node.getName(): {}", node.getName());
							}
							
							@Override
							public void meet(ValueConstant node) throws RuntimeException {
								String type = node.getValue().toString();
								res.add(type.substring(type.lastIndexOf("/") + 1));
								logger.debug("--- type: {}", type);
							}
						});
					}
					
					@Override
					public void meet(Var node) throws RuntimeException {
						res.add(node.getName());
						logger.debug("--- OUTSIDE node.getName(): {}", node.getName());
					}
					
					@Override
					public void meet(ValueConstant node) throws RuntimeException {
						String value = node.getValue().toString();
						res.add(value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
						logger.debug("--- value: {}", value);
					}
				});
				
				allRes.add(res);
			}
		});
		return allRes;
	}
	
	
	protected static List<String> computeTriples(TupleExpr serviceExpression) {
		final List<String> res = new ArrayList<String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
		
			@Override
			public void meet(Var node)
			throws RuntimeException {
				// take all expr vars
				if (node.getParentNode().getClass().toString().contains("StatementPattern")) {
					if (!node.hasValue() && !node.isAnonymous())
						res.add(node.getName());
					else 
						res.add(node.getValue().toString());
				}
			}
		});
		return res;
	}
	
	
	protected static String serializeList(List<String> list, String separator, String start) {
		String string = "";
		for (String l : list) {
			if (!string.equals("")) string += separator;
			else string += start;
			string += l;
		}
		return string;
	}
	
	protected static String serializeSet(Set<String> set, String separator, String start) {
		String string = "";
		for (String s : set) {
			if (!string.equals("")) string += separator;
			else string += start;
			string += s;
		}
		return string;
	}

	
	protected static void keepWKTAndTypePredicates(List<String> triples) {
		int n = 1;
		while (n < triples.size()) {
			if (!triples.get(n).contains("#asWKT") && !triples.get(n).contains("#type")) {
				triples.remove(n+1);
				triples.remove(n);
				triples.remove(n-1);
			}
			else {
				n += 3;
			}
		}
	}
	
	
	protected static Pair<String, String> subjectDecompose(String subject) {
		int idStart = subject.lastIndexOf("/") + 1;
		String id = subject.substring(idStart);
		int lastDot = subject.lastIndexOf(".");
		int tableStart = lastDot + subject.substring(lastDot).indexOf("/") + 1;
		int tableEnd = tableStart + subject.substring(tableStart).indexOf("/");
		String table = subject.substring(tableStart, tableEnd);

//		int tableEnd = subject.substring(0, idStart - 2).lastIndexOf("/");
//		int tableStart = subject.substring(0, tableEnd).lastIndexOf("/") + 1;
//		String table = subject.substring(tableStart, tableEnd);
		
		if (table.equals("pgm"))
			table = DEFAULT_TABLE_NAME;
		
		return Pair.of(table, id);
	}
	
	
	protected static String decomposeToId(String string) {
		int idStart = string.lastIndexOf("/") + 1;
		String id = string.substring(idStart);
		return id;
	}
	
}
