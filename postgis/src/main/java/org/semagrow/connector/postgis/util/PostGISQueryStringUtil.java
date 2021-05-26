package org.semagrow.connector.postgis.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
//	private static String INDEX_BINDING_NAME = ".__id";
	private static String INDEX_BINDING_NAME = ".id";
	private static String WKT_BINDING_NAME = ".wkt";
	private static String TABLE = " geometries ";
	private static boolean ONE_TABLE = true;
	private static String SELECT = "SELECT ";
	private static String FROM = " FROM ";
	private static String WHERE = " WHERE ";
	private static String UNION = " UNION ";
	private static String COMMA_SEP = ", ";
	private static String AND_SEP = " AND ";
	
	
	public static String buildSQLQuery(TupleExpr expr, Set<String> freeVars, List<String> tables, BindingSet bindings) {
		
		List<String> triples = computeTriples(expr);
//		List<String> filterInfo = computeFilterVars(expr);
//		List<String> bindInfo = computeBindVars(expr);
		
		// remove all triples with predicate different than #asWKT
		keepOnlyAsWKTPredicates(triples);
		if (triples.isEmpty()) return null;
		
		logger.debug("triples: {}", triples);
//		logger.debug("filter variables: {} ", filterInfo);
//		logger.debug("bind variables: {} ", bindInfo);
		
		Map<String,String> bindingVars = new HashMap<String, String>();
		
		Set<String> bindingNames = bindings.getBindingNames();
		for (Object binding: bindingNames) {
			logger.debug("bindings: {} - {}", binding, bindings.getValue((String)binding));
			if (triples.contains(binding.toString())) {
				int index = triples.indexOf(binding);
				triples.remove(index);
				triples.add(index, bindings.getValue((String)binding).toString());
			}
			bindingVars.put((String) binding, bindings.getValue((String)binding).toString().replace("\"", "\'"));
		}
		
		logger.debug("triples 2: {}", triples.toString());
		
		List<String> select = new ArrayList<String>();
		List<String> from = new ArrayList<String>();
		List<String> where = new ArrayList<String>();
		
		Pair<String, String> tableAndΙd = null;
		String wkt = "";
		int place = 0;
		
		for (String part : triples) {
			place++;
			if (part.contains("#")) {	// predicate
				wkt = "t" + (place-1) + WKT_BINDING_NAME;
			}
			else {			
				if (place % 3 != 0) {	// subject
					if (freeVars.contains(part)) {	// free var as subject (from ?)
						select.add("t" + place + INDEX_BINDING_NAME + " AS " + part);
						bindingVars.put(part, "t"+ place + INDEX_BINDING_NAME);
						tables.add("?");
					}
					else { 							// binding var as subject
						tableAndΙd = subjectDecompose(part);
						from.add(tableAndΙd.getLeft() + " t" + place);
						where.add("t" + place + INDEX_BINDING_NAME + " = " + tableAndΙd.getRight());
					}
				}
				else {					// object
					if (freeVars.contains(part)) {	// free var as object
						select.add("ST_AsText(" + wkt + ") AS " + part);
						bindingVars.put(part, wkt);
						tables.add("-");
					}
					else {							// binding var as object
						where.add("ST_Equals(t" + place + WKT_BINDING_NAME + ", ST_GeomFromText('" + part + "',4326))");
						if (tables.get(place/3 - 1).equals("?"))
							tables.remove(place/3 - 1);
						if (part.contains("POINT")) {
							tables.add("lucas");
						}
						else if (part.contains("MULTIPOLYGON")) {
							tables.add("invekos");
						}
						else if (part.contains("POLYGON")) {
							tables.add(TABLE);
						}
					}
					
				}
			}
		}
		
		logger.debug("select:: {}", select);
		logger.debug("from:: {}", from);
		logger.debug("where:: {}", where);
		logger.debug("tables:: {}", tables);
		
		
		String sqlQuery = null;
//		if (vars.size() - 1 > triples.size() / 3)  {	//throw exception ????
//			logger.error("More than one triples with two free variables.");
//			throw new QueryEvaluationException();
//		}
		
		Set<String> binds = new HashSet<String>();
		List<String> filters = new ArrayList<String>();
		
		computeBindsAndFilters(expr, triples, binds, filters, tables, bindingVars);
		
		logger.debug("binds:: {}", binds);
		logger.debug("filters:: {}", filters);
		
		for (String b : binds)
			select.add(b);
		
		for (String f : filters)
			where.add(f);
		
		if (from.isEmpty()) {
			if (ONE_TABLE) {
				sqlQuery = serializeList(select, COMMA_SEP, SELECT) 
						+ " FROM geometries t1" 
						+ serializeList(where, AND_SEP, WHERE) + ";";
			}
			else {
				String from1 = "", from2 = "", from3 = "", from4 = "";
				logger.debug("vars size: {}", freeVars.size());
				logger.debug("triples size: {}", triples.size());
				logger.debug("bindingVars size: {}", bindingVars.size());
				logger.debug("bindingVars: {}", bindingVars);
				if (triples.size() == 3) {
					from1 = " FROM lucas t1";
					from2 = " FROM invekos t1";
					sqlQuery = serializeList(select, COMMA_SEP, SELECT) + from1 
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from2
							+ serializeList(where, AND_SEP, WHERE) + ";";
				}
				else if (triples.size() == 6) {
					from1 = " FROM lucas t1, lucas t4";
					from2 = " FROM lucas t1, invekos t4";
					from3 = " FROM invekos t1, lucas t4";
					from4 = " FROM invekos t1, invekos t4";
					sqlQuery = serializeList(select, COMMA_SEP, SELECT) + from1 
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from2
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from3
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from4
							+ serializeList(where, AND_SEP, WHERE) + ";";
				}
			}
		}
		else {
			if (ONE_TABLE) {
				sqlQuery = serializeList(select, COMMA_SEP, SELECT) 
						+ serializeList(from, COMMA_SEP, FROM)
						+ serializeList(where, AND_SEP, WHERE) + ";";
			}
			else {
				if (freeVars.size() == triples.size() / 3 && !triples.contains(null)) {
					sqlQuery = serializeList(select, COMMA_SEP, SELECT) 
							+ serializeList(from, COMMA_SEP, FROM)
							+ serializeList(where, AND_SEP, WHERE) + ";";
				}
				else {
					String lucas = "", invekos = "";
					place = 1;
					while (place < triples.size()) {
						if (!from.contains("lucas t" + place) && !from.contains("invekos t" + place)) {
							lucas += ", lucas t" + place;
							invekos += ", invekos t" + place;
						}
						place += 3;
					}
					sqlQuery = serializeList(select, COMMA_SEP, SELECT) 
							+ serializeList(from, COMMA_SEP, FROM) + lucas
							+ serializeList(where, AND_SEP, WHERE) + UNION
							+ serializeList(select, COMMA_SEP, SELECT) 
							+ serializeList(from, COMMA_SEP, FROM) + invekos
							+ serializeList(where, AND_SEP, WHERE) + ";";
				}
			}
		}
		
		logger.debug("2 bindingVars:: {}", bindingVars.toString());
		logger.debug("sqlQuery:: {}", sqlQuery);
		return sqlQuery;
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
	
	
	public static String buildSQLQueryUnion(TupleExpr expr, Set<String> freeVars, List<String> tables, List<BindingSet> bindings, Collection<String> relevantBindingNames) {
		
		List<String> triples = computeTriples(expr);
		List<String> filterInfo = computeFilterVars(expr);
		List<String> bindInfo = computeBindVars(expr);
		
		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
			logger.error("More than one triples with two free variables.");
			throw new QueryEvaluationException();
		}
		
		keepOnlyAsWKTPredicates(triples);
		if (triples.isEmpty()) return null;
		
		logger.debug("triples: {}", triples);
		logger.debug("filter variables: {} " ,filterInfo);
		logger.debug("bind variables: {} " ,bindInfo);
		
		Map<String,String> bindingVars = new HashMap<String, String>();
		
//		logger.debug("filterVars: {}", filterVars.toString());
		
//		Set<String> bindingNames = bindings.getBindingNames();
//		for (Object binding: bindingNames) {
//			logger.debug("bindings: {} - {}", binding, bindings.getValue((String)binding));
//			if (triples.contains(binding.toString())) {
//				int index = triples.indexOf(binding);
//				triples.remove(index);
//				triples.add(index, bindings.getValue((String)binding).toString());
//			}
////			asToVar.put((String) binding, bindings.getValue((String)binding).toString().replace("\"", "\'"));
//		}
		
		Pair<String, String> tableAndΙd = null;
		List<String> select = new ArrayList<String>();
		List<String> from = new ArrayList<String>();
		List<String> where = new ArrayList<String>();
		String wkt = "", binding_var = "";
		int binding_place = -1;
		int place = 0;
		
		for (String part : triples) {
			place++;
			if (part.contains("#")) {	// predicate
				wkt = "t" + (place-1) + WKT_BINDING_NAME;
			}
			else {			
				if (place % 3 != 0) {	// subject
					if (freeVars.contains(part)) {	// free var as subject (from ?)
						select.add("t"+ place + INDEX_BINDING_NAME + " AS " + part);
						bindingVars.put(part, "t"+ place + INDEX_BINDING_NAME);
						tables.add("?");
					}
					else if (bindings.get(0).hasBinding(part)) {
						tableAndΙd = subjectDecompose(bindings.get(0).getBinding(part).getValue().toString());
						from.add(tableAndΙd.getLeft() + " t" + place);
						binding_var = part;
						binding_place = place;
					}
					else {
						tableAndΙd = subjectDecompose(part);
						from.add(tableAndΙd.getLeft() + " t" + place);
						where.add("t" + place + INDEX_BINDING_NAME + " = " + tableAndΙd.getRight());
					}
				}
				else {					// object
					if (freeVars.contains(part)) {
						select.add("ST_AsText(" + wkt + ") AS " + part);
						bindingVars.put(part, wkt);
						tables.add("-");
					}
					else {
						where.add("ST_Equals(t" + place + WKT_BINDING_NAME + ", ST_GeomFromText('" + part + "',4326))");
						if (tables.get(place/3 - 1).equals("?"))
							tables.remove(place/3 - 1);
						if (part.contains("POINT")) {
							tables.add("lucas");
						}
						else if (part.contains("MULTIPOLYGON")) {
							tables.add("invekos");
						}
						else if (part.contains("POLYGON")) {
							tables.add(TABLE);
						}
					}
					
				}
			}
		}
		
		logger.debug("select:: {}", select);
		logger.debug("from:: {}", from);
		logger.debug("where:: {}", where);
		logger.debug("tables:: {}", tables.toString());
		
		
		String sqlQuery = "";
//		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
//			logger.error("More than one triples with two free variables.");
//			throw new QueryEvaluationException();
//		}
		
		Set<String> binds = new HashSet<String>();
		List<String> filters = new ArrayList<String>();
		
		computeBindsAndFilters(expr, triples, binds, filters, tables, bindingVars);
		
		logger.debug("binds:: {}", binds);
		logger.debug("filters:: {}", filters);
		
		for (String b : binds)
			select.add(b);
		
		for (String f : filters)
			where.add(f);
		
		
		if (from.isEmpty()) {
			if (ONE_TABLE) {
				sqlQuery = serializeList(select, COMMA_SEP, SELECT) 
						+ " FROM geometries t1" 
						+ serializeList(where, AND_SEP, WHERE) + ";";
			}
			else {
				String from1 = "", from2 = "", from3 = "", from4 = "";
				logger.debug("vars size: {}", freeVars.size());
				logger.debug("triples size: {}", triples.size());
				logger.debug("asToVar size: {}", bindingVars.size());
				if (triples.size() == 3) {
					from1 = " FROM lucas t1";
					from2 = " FROM invekos t1";
					sqlQuery = serializeList(select, COMMA_SEP, SELECT) + from1 
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from2
							+ serializeList(where, AND_SEP, WHERE) + ";";
				}
				else if (triples.size() == 6) {
					from1 = " FROM lucas t1, lucas t4";
					from2 = " FROM lucas t1, invekos t4";
					from3 = " FROM invekos t1, lucas t4";
					from4 = " FROM invekos t1, invekos t4";
					sqlQuery = serializeList(select, COMMA_SEP, SELECT) + from1 
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from2
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from3
							+ serializeList(where, AND_SEP, WHERE) + UNION 
							+ serializeList(select, COMMA_SEP, SELECT) + from4
							+ serializeList(where, AND_SEP, WHERE) + ";";
				}
			}
		}
		else {
			if (ONE_TABLE) {
				logger.debug("WHAT TO DO HERE ?????");
			}
			else {
				logger.debug("vars size: {}", freeVars.size());
				logger.debug("triples size: {}", triples.size());
				
				String where_bind = "";
				
				logger.debug("now sqlQuery: {}", sqlQuery);
				
				for (BindingSet b : bindings) {
					if (!sqlQuery.equals(""))
						sqlQuery += UNION;
					
					tableAndΙd = subjectDecompose(b.getBinding(binding_var).getValue().toString());
					where_bind = "t" + binding_place + INDEX_BINDING_NAME + " = " + tableAndΙd.getRight();	
					
					if (freeVars.size() == triples.size() / 3 && !triples.contains(null)) {
						sqlQuery += serializeList(select, COMMA_SEP, SELECT) 
								+ serializeList(from, COMMA_SEP, FROM)
								+ serializeList(where, AND_SEP, WHERE);
						if (where.isEmpty()) sqlQuery += WHERE;
						else sqlQuery += AND_SEP;
						sqlQuery += where_bind;
					}
					else {
						String lucas = "", invekos = "";
						place = 1;
						while (place < triples.size()) {
							if (!from.contains("lucas t" + place) && !from.contains("invekos t" + place)) {
								lucas += ", lucas t" + place;
								invekos += ", invekos t" + place;
							}
							place += 3;
						}
						sqlQuery += serializeList(select, COMMA_SEP, SELECT) 
								+ serializeList(from, COMMA_SEP, FROM) + lucas
								+ serializeList(where, AND_SEP, WHERE);
						if (where.isEmpty()) sqlQuery += WHERE;
						else sqlQuery += AND_SEP;
						sqlQuery += where_bind + UNION;
						sqlQuery += serializeList(select, COMMA_SEP, SELECT) 
								+ serializeList(from, COMMA_SEP, FROM) + invekos
								+ serializeList(where, AND_SEP, WHERE);
						if (where.isEmpty()) sqlQuery += WHERE;
						else sqlQuery += AND_SEP;
						sqlQuery += where_bind;
					}
				}
				
				sqlQuery += ";";
			}
		}
		
		logger.debug("bindingVars:: {}", bindingVars.toString());
		logger.debug("sqlQuery:: {}", sqlQuery);
		return sqlQuery;
	}
	
	
	protected static void computeBindsAndFilters(TupleExpr expr, List<String> triples, Set<String> binds, List<String> filters, List<String> tables, Map<String,String> bindingVars) {
		List<String> filterInfo = computeFilterVars(expr);
		List<String> bindInfo = computeBindVars(expr);
		
		logger.debug("filter variables: {} ", filterInfo);
		logger.debug("bind variables: {} ", bindInfo);
		
		String dist = "";
		String compare = "", function = "", type = "";
		int i = 0;
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
				function = filterInfo.get(i);
				if (function.equals("distance")) dist += "ST_Distance(";
				type = filterInfo.get(i+3);
				if (type.equals("metre")) dist += "ST_GeographyFromText(ST_AsText(";
				else if (type.equals("degree")) dist += "ST_GeomFromText(ST_AsText(";
				if (bindingVars.containsKey(filterInfo.get(i+1))) {
					String var = bindingVars.get(filterInfo.get(i+1));
					if (var.contains("^")) var = var.substring(0, var.indexOf("^"));
					logger.debug("--- bindingVars: {}", var);
					dist += var;
				}
				else {
					bindingVars.put(filterInfo.get(i+1), "t"+(triples.size()+1)+WKT_BINDING_NAME);
					dist += bindingVars.get(filterInfo.get(i+1));
					triples.add(null); triples.add(null); triples.add(null);
				}
				logger.debug("--- dist: {}", dist);
				if (type.equals("metre")) dist += ")), ST_GeographyFromText(ST_AsText(";
				else if (type.equals("degree")) dist += "), 4326), ST_GeomFromText(ST_AsText(";
				if (bindingVars.containsKey(filterInfo.get(i+2))) {
					String var = bindingVars.get(filterInfo.get(i+2));
					if (var.contains("^")) var = var.substring(0, var.indexOf("^"));
					logger.debug("--- bindingVars: {}", var);
					dist += var;
				}
				else {
					bindingVars.put(filterInfo.get(i+2), "t"+(triples.size()+1)+WKT_BINDING_NAME);
					dist += bindingVars.get(filterInfo.get(i+2));
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (type.equals("metre")) dist += "))";
				else if (type.equals("degree")) dist += "), 4326)";
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
	
	
	protected static List<String> computeFilterVars(TupleExpr serviceExpression) {
		final List<String> res = new ArrayList<String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(Compare node) throws RuntimeException {
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
								res.add(type.substring(type.lastIndexOf("/") + 1));
							}
						});
					}
					
					@Override
					public void meet(ValueConstant node) throws RuntimeException {
						String value = node.getValue().toString();
						res.add(value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
					}
				});
			}
		});
		return res;
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
	
	
	protected static void keepOnlyAsWKTPredicates(List<String> triples) {
		int n = 1;
		while (n < triples.size()) {
			if (!triples.get(n).contains("#asWKT")) {
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
			table = TABLE;
		
		return Pair.of(table, id);
	}
	
}
