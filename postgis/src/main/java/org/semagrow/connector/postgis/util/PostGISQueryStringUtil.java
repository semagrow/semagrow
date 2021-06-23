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
		
	private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);
	private static String TYPE_URI;
	private static String INDEX_BINDING_NAME = ".id";
	private static String WKT_BINDING_NAME = ".wkt";
	private static String DEFAULT_TABLE_NAME = "geometries";
	private static String SRID = "4326";
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
	
	private static String asText(String geom) {
		return "ST_AsText(" + geom + ")";
	}
	
	private static String geomFromText(String wkt) {
		return "ST_GeomFromText(" + wkt + ", " + SRID + ")";
	}
	
	private static String geographyFromText(String wkt) {
		return "ST_GeographyFromText(" + wkt + ")";
	}
	
	private static String stEquals(String wkt1, String wkt2) {
		return "ST_Equals(" + wkt1 + ", " + wkt2 + ")";
	}
	
	private static String simpleFeatureFunction(String call) {
		String sfFunction = null;
		switch(call) {
		  case "sfEquals":
			  sfFunction = "ST_Equals"; break;
		  case "sfDisjoint":
			  sfFunction = "ST_Disjoint"; break;
		  case "sfIntersects":
			  sfFunction = "ST_Intersects"; break;
		  case "sfTouches":
			  sfFunction = "ST_Disjoint"; break;
		  case "sfCrosses":
			  sfFunction = "ST_Crosses"; break;
		  case "sfWithin":
			  sfFunction = "ST_Within"; break;
		  case "sfContains":
			  sfFunction = "ST_Contains"; break;
		  case "sfOverlaps":
			  sfFunction = "ST_Overlaps"; break;
		}
		return sfFunction;
	}
	
	private static String nonTopologicalFunction(String call) {
		String function = null;
		switch(call) {
		  case "distance":
			  function = "ST_Distance"; break;
		}
		return function;
	}
	
	private static String function(String call, String geom1, String geom2) {
		return call + "(" + geom1 + ", " + geom2 + ")";
	}
	
	public static void createTripleMaps(List<String> triples, Map<String, String> wktMap, 
			Map<String, String> typeMap) {
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
	}
	
	/**
	 * Replace variables in each triple with their bindings derived from filter clauses. 
	 * Remove these variables from the bindingVars map.
	 */
	public static void replaceVarsWithFilterBindings(Set<List<String>> allFilters, Set<String> freeVars, 
			List<String> triples, Map<String,String> bindingVars) {
		String var1, var2, operator;
		for (List<String> filterInfo : allFilters) {
			if (filterInfo.size() == 3 && !filterInfo.get(0).contains("sf")) {
				operator = filterInfo.get(0);
				var1 = filterInfo.get(1);
				var2 = filterInfo.get(2);
				if ((freeVars.contains(var1) && freeVars.contains(var2)) || freeVars.contains(var2)) {
					if (operator.equals("="))
						replacement(triples, var2, var1);
//					bindingVars.remove(bindingVars.remove(var2));
					if (!bindingVars.get(var1).isEmpty() && !bindingVars.get(var2).isEmpty()) 
						logger.debug("what happens if bindingVars contains both variables?");
				}
				else if (freeVars.contains(var1)) {
					if (operator.equals("="))
						replacement(triples, var1, var2);
					bindingVars.remove(bindingVars.remove(var1));
				}
				else {
					logger.debug("what happens if both are constant values?");
				}
			}
		}
	}
	
	private static void replacement(List<String> varList, String var1, String var2) {
		while (varList.contains(var1)) {
			int index = varList.indexOf(var1);
			varList.remove(index);
			varList.add(index, var2);
		}
	}
	
	public static String keepBinding(Map<String,String> bindingVars, String var) {
		if (bindingVars.containsKey(var)) {
			var = bindingVars.get(var);
			if (var.contains("^")) var = var.substring(1, var.indexOf("^"));
		}
		return var;
	}
	
	/**
	 * @return all conditions derived from the filter clauses.
	 * ((( Probably boolean queryUnion is not needed )))
	 */
	public static Set<String> computingFilterClauses(Set<List<String>> allFilters, 
			Map<String,String> bindingVars, boolean queryUnion) {
		
		String operator, call, var1, var2, unit, value, condition = "";
		Set<String> allConditions = new HashSet<String>();
		
		for (List<String> filterInfo : allFilters) {
			if (filterInfo.size() == 3 && !filterInfo.get(0).contains("sf")) {
				continue;
			}
			else if (filterInfo.size() == 3 && filterInfo.get(0).contains("sf")) {
				call = filterInfo.get(0);
				var1 = keepBinding(bindingVars, filterInfo.get(1));
				var2 = keepBinding(bindingVars, filterInfo.get(2));
				
				condition = function(simpleFeatureFunction(call), 
						geomFromText(asText(var1)), geomFromText(asText(var2)));
			}
			else if (filterInfo.size() == 6) {
				operator = filterInfo.get(0);
				call = filterInfo.get(1);
				var1 = keepBinding(bindingVars, filterInfo.get(2));
				var2 = keepBinding(bindingVars, filterInfo.get(3));
				unit = filterInfo.get(4);
				value = filterInfo.get(5);

				if (unit.equals("metre")) {
					condition = function(nonTopologicalFunction(call), 
							geographyFromText(asText(var1)), geographyFromText(asText(var2)));
				}
				else if (unit.equals("degree")) {
					condition = function(nonTopologicalFunction(call), 
							geomFromText(asText(var1)), geomFromText(asText(var2)));
				}
				condition += " " + operator + " " + value;
			}

			allConditions.add(condition);
		}
		
		return allConditions;
	}
	
	private static String getValueFromLiteralOrIRI(BindingSet bindings, String bindingName) {
		String value = bindings.getValue(bindingName).stringValue();
		if (!value.contains("http"))
			value = "'" + value + "'";
		return value;
	}
	
	/**
	 * Replace variables in each triple with their bindings. 
	 * @return a map from variables to their binding.
	 */
	private static Map<String, String> replaceVarsWithBindings(BindingSet bindings, 
			List<String> triples) {
		
		Map<String,String> bindingVars = new HashMap<String, String>();
		
		Set<String> bindingNames = bindings.getBindingNames();
		for (String bindingName: bindingNames) {
			String value = getValueFromLiteralOrIRI(bindings, bindingName);
			replacement(triples, bindingName, value);
			bindingVars.put(bindingName, value);
		}
		
		return bindingVars;
	}
	
	public static String buildUnion(List<BindingSet> bindingsList, Collection<String> relevantBindingNames, 
			String sqlQuery) {
		
		String sqlQueryUnion = "";
		
		String transformingSqlQuery;
		for (BindingSet bindings: bindingsList) {
			transformingSqlQuery = sqlQuery;
			for (String relevantBindingName: relevantBindingNames) {
				if (sqlQuery.contains(relevantBindingName)) {
					String value = getValueFromLiteralOrIRI(bindings, relevantBindingName);
					transformingSqlQuery = transformingSqlQuery.replace(relevantBindingName, value);
				}
			}
			sqlQueryUnion += transformingSqlQuery + UNION;
		}
		
		// remove the last "UNION" operator
		sqlQueryUnion = sqlQueryUnion.substring(0, sqlQueryUnion.length() - 6) + ";";
		return sqlQueryUnion;
	}
	
	public static String buildSimpleSQLQuery(Set<String> freeVars, Map<String,String> bindingVars, 
			Map<String,String> extraBindingVars, List<String> triples) {

		Map<String, String> wktMap = new HashMap<String, String>();
		Map<String, String> typeMap = new HashMap<String, String>();
		createTripleMaps(triples, wktMap, typeMap);
		
		Set<String> selectSet = new HashSet<String>();
		Set<String> fromSet = new HashSet<String>();
		Set<String> whereSet = new HashSet<String>();
		
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
				selectSet.add(asText(G + i + WKT_BINDING_NAME) + AS + wkts.getValue());
				bindingVars.put(wkts.getValue(), G + i + WKT_BINDING_NAME);
			}
			else {
				if (!freeVars.contains(wkts.getKey())) selectSet.add(G + i + INDEX_BINDING_NAME);
				whereSet.add(stEquals(G + i + WKT_BINDING_NAME, geomFromText(wkts.getValue().replace("\"", "\'"))));
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
					logger.debug("wrong type uri - return NULL -> {} - {}", types.getValue(), TYPE_URI); //return NULL ???
			}
			i++;
		}
		String query = serializeSet(selectSet, COMMA_SEP, SELECT) 
				+ serializeSet(fromSet, COMMA_SEP, FROM)
				+ serializeSet(whereSet, AND_SEP, WHERE);
				
		return query;
	}
	
	
	public static String buildSQLQuery(TupleExpr expr, Set<String> freeVars, List<String> tables, 
			BindingSet bindings, Map<String,String> extraBindingVars, String dbname) {
				
		typeURI(dbname);
		List<String> triples = computeTriples(expr);
		
		triplePruning(triples);
		if (triples.isEmpty()) return null;
				
		Map<String,String> bindingVars = replaceVarsWithBindings(bindings, triples);
		
		Set<List<String>> allFilters = computeFilterVars(expr);
		replaceVarsWithFilterBindings(allFilters, freeVars, triples, bindingVars);
		
//		List<String> bindInfo = computeBindVars(expr);
		
		String sqlQuery = buildSimpleSQLQuery(freeVars, bindingVars, extraBindingVars, triples);
		
		if (sqlQuery.contains(WHERE))
			sqlQuery += serializeSet(computingFilterClauses(allFilters, bindingVars, false), AND_SEP, AND_SEP);
		else 
			sqlQuery += serializeSet(computingFilterClauses(allFilters, bindingVars, false), AND_SEP, WHERE);
		
		return sqlQuery;
	}
	
	
	public static String buildSQLQueryUnion(TupleExpr expr, Set<String> freeVars, 
		List<String> tables, List<BindingSet> bindings, Collection<String> relevantBindingNames, 
		Map<String,String> extraBindingVars, String dbname) {
				
		typeURI(dbname);
		List<String> triples = computeTriples(expr);
		
		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
			logger.error("More than one triples with two free variables.");
			throw new QueryEvaluationException();
		}
		
		triplePruning(triples);
		if (triples.isEmpty()) return null;
		
		Set<List<String>> allFilters = computeFilterVars(expr);
		Map<String,String> bindingVars = new HashMap<String, String>();
		replaceVarsWithFilterBindings(allFilters, freeVars, triples, bindingVars);
		
//		List<String> bindInfo = computeBindVars(expr);
		
		String sqlQuery = buildSimpleSQLQuery(freeVars, bindingVars, extraBindingVars, triples);
				
		if (sqlQuery.contains(WHERE))
			sqlQuery += serializeSet(computingFilterClauses(allFilters, bindingVars, true), AND_SEP, AND_SEP);
		else
			sqlQuery += serializeSet(computingFilterClauses(allFilters, bindingVars, true), AND_SEP, WHERE);

		return buildUnion(bindings, relevantBindingNames, sqlQuery);
	}
	
	
	/**
	 * Compute all binds in serviceExpression 
	 */
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
	

	/**
	 * Compute all filters in serviceExpression 
	 */
	protected static Set<List<String>> computeFilterVars(TupleExpr serviceExpression) {
		final Set<List<String>> allRes = new HashSet<List<String>>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(Compare node) throws RuntimeException {
				final List<String> res = new ArrayList<String>();
				// take all info about compare nodes, i.e. operation, vars, values etc
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
								res.add(type.substring(type.lastIndexOf("/") + 1).replace("\"", "'"));
							}
						});
					}
					
					@Override
					public void meet(Var node) throws RuntimeException {
						res.add(node.getName());
					}
					
					@Override
					public void meet(ValueConstant node) throws RuntimeException {
						String value = node.getValue().toString();
						res.add(value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
					}
				});
				
				allRes.add(res);
			}
			
			@Override
			public void meet(FunctionCall node) throws RuntimeException {
				final List<String> res = new ArrayList<String>();
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
						res.add(type.substring(type.lastIndexOf("/") + 1).replace("\"", "'"));
					}
				});
				
				allRes.add(res);
			}
			
		});
		return allRes;
	}
	
	/**
	 * Compute all triples (subject predicate object) in serviceExpression 
	 */
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
	
	/**
	 * First add simple feature functions and then distance operators in SQL query.
	 * @return empty string or a serialized set.
	 */
	protected static String serializeSet(Set<String> set, String separator, String start) {
		String string = "";
		Set<String> distances = new HashSet<String>();
		for (String s : set) {
			if (s.contains("ST_Distance"))
				distances.add(s);
			else {
				if (!string.equals("")) string += separator;
				else string += start;
				string += s;
			}
		}
		for (String s : distances) {
			if (!string.equals("")) string += separator;
			else string += start;
			string += s;
		}
		return string;
	}

	
	/**
	 * Removes all triples with predicate different than #asWKT or #type 
	 */
	protected static void triplePruning(List<String> triples) {
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
	
	/**
	 * @return table and id names.
	 */
	protected static Pair<String, String> subjectDecompose(String subject) {
		int idStart = subject.lastIndexOf("/") + 1;
		String id = subject.substring(idStart);
		int lastDot = subject.lastIndexOf(".");
		int tableStart = lastDot + subject.substring(lastDot).indexOf("/") + 1;
		int tableEnd = tableStart + subject.substring(tableStart).indexOf("/");
		String table = subject.substring(tableStart, tableEnd);
		
		if (table.equals("pgm"))
			table = DEFAULT_TABLE_NAME;
		
		return Pair.of(table, id);
	}
	
	/**
	 * @return the id.
	 */
	protected static String decomposeToId(String string) {
		int idStart = string.lastIndexOf("/") + 1;
		String id = string.substring(idStart);
		return id;
	}
	
}
