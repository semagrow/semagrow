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
	private static String INDEX_BINDING_NAME = "__id";
	
	
	public static String buildSQLQuery(TupleExpr expr, Set<String> vars, List<String> tables, BindingSet bindings) {
		
		Pair<String, String> tableAndGid = null;
		
		List<Map<String,String>> filterVars = new ArrayList<Map<String,String>>();
		filterVars.add(computeFunctionCallVars(expr));
		
		List<String> fv = computeFilterVars(expr);
		logger.info("filter variables: {} " ,fv);
		logger.info("computeFilterVars!!!! DONE");
		
		List<String> bv = computeBindVars(expr);
		logger.info("bind variables: {} " ,bv);
		logger.info("computeBindVars!!!! DONE");
		
		List<String> triples = computeTriples(expr);
		
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
		
		if (triples.isEmpty())
			return null;
		
		Map<String,String> asToVar = new HashMap<String, String>();
		logger.info("BEFORE triples: {}", triples.toString());
		logger.info("filterVars: {}", filterVars.toString());
		
		Set<String> bindingNames = bindings.getBindingNames();
		for (Object binding: bindingNames) {
			logger.info("bindings: {} - {}", binding, bindings.getValue((String)binding));
			if (triples.contains(binding.toString())) {
				int index = triples.indexOf(binding);
				triples.remove(index);
				triples.add(index, bindings.getValue((String)binding).toString());
			}
			asToVar.put((String) binding, bindings.getValue((String)binding).toString().replace("\"", "\'"));
		}
		
		logger.info("AFTER triples: {}", triples.toString());
		logger.info("AFTER bv: {}", bv.toString());
		logger.info("AFTER fv: {}", fv.toString());
		
		String select = "SELECT ", where = " WHERE ", from = " FROM ", geom = "";
		int place = 0;
		
		for (String part : triples) {
			place++;
			if (part.contains("#")) {	//predicate
////				if (!select.equals("SELECT ")) select += ", ";
////				select += predDecompose(part, place - 1);
//				geom = predDecompose(part, place - 1);
				if (part.substring(part.lastIndexOf("#") + 1).equals("asWKT")) {
					geom = "t" + (place-1) + ".geom";
				}
				else {
					logger.error("No \"asWKT\" predicate.");
//					return null;
//					throw new QueryEvaluationException();
				}
			}
			else {			
				if (place % 3 != 0) {	//subject
					if (vars.contains(part)) {
						if (!select.equals("SELECT ")) select += ", ";
						select += "t"+ place + ".gid AS " + part;
						asToVar.put(part, "t"+ place + ".gid");
						tables.add("?");
					}
					else {				
						tableAndGid = subDecompose(part);
						if (!from.equals(" FROM ")) from += ", ";
						from += tableAndGid.getLeft() + " t" + place;
						if (!where.equals(" WHERE ")) where += " AND ";
						where += "t" + place + ".gid = " + tableAndGid.getRight();
//						tables.add(tableAndGid.getLeft());
					}
				}
				else {					//object
					if (vars.contains(part)) {
						if (!select.equals("SELECT ")) select += ", ";
						select += "ST_AsText(" + geom + ") AS " + part;
						asToVar.put(part, geom);
						tables.add("-");
					}
					else {
						if (!where.equals(" WHERE ")) where += " AND ";
						where += "ST_Equals(t" + place 
								+ ".geom, ST_ST_GeomFromText('" + part + "',4326))" ;
						if (tables.get(place/3 - 1).equals("?"))
							tables.remove(place/3 - 1);
						if (part.contains("POINT")) {
							tables.add("lucas");
						}
						else if (part.contains("MULTIPOLYGON")) {
							tables.add("invekos");
						}
					}
					
//					ST_Equals(geom,ST_GeomFromText('POINT(16.25613715 47.5043295)',4326));
				}
			}
		}
		
		logger.info("select:: {}", select);
		logger.info("from:: {}", from);
		logger.info("where:: {}", where);
		logger.info("tables:: {}", tables.toString());
		
		
		String sqlQuery = null;
//		if (vars.size() - 1 > triples.size() / 3)  {	//throw exception ????
//			logger.error("More than one triples with two free variables.");
//			throw new QueryEvaluationException();
//		}
		
		logger.info("1 asToVar:: {}", asToVar.toString());
		String dist = "", filter = "", bind = "";
//		if (!filterVars.isEmpty()) {
//		while (!filterVars.isEmpty()) {
//			Map<String, String> filt = filterVars.remove(0);
//			if (filt.get("function").equals("distance"))
//				dist += "ST_Distance(";
//			if (filt.get("type").equals("metre")) {
////				dist += "ST_GeographyFromText(ST_AsText("
////						+ asToVar.get(filterVars.get("var"))
////						+ ")), ST_GeographyFromText(ST_AsText("
////						+ asToVar.get(filterVars.get("var2"))
////						+ "))";
//				dist += "ST_GeographyFromText(ST_AsText(";
//				if (asToVar.containsKey(filt.get("var"))) dist += asToVar.get(filt.get("var"));
//				else {
//					asToVar.put(filt.get("var"), "t"+(triples.size()+1)+".geom");
//					dist += asToVar.get(filt.get("var"));
//					triples.add(null); triples.add(null); triples.add(null);
//				}
//				dist += ")), ST_GeographyFromText(ST_AsText(";
//				if (asToVar.containsKey(filt.get("var2"))) dist += asToVar.get(filt.get("var2"));
//				else {
//					asToVar.put(filt.get("var2"), "t"+(triples.size()+1)+".geom");
//					dist += asToVar.get(filt.get("var2"));
//					triples.add(null); triples.add(null); triples.add(null);
//				}
//				dist += "))";
//			}
//			else if (filt.get("type").equals("degree")) {
//				dist += asToVar.get(filt.get("var")) 
//						+ ", " + asToVar.get(filt.get("var2"));
//			}
//			dist += ") ";
//			logger.info("dist:: {}", dist);
//			
//			if (filt.containsKey("signature")) {
//				if (!select.equals("SELECT ")) select += ", ";
//				bind += dist + "AS " + filt.get("signature");
//			}
//			if (filt.containsKey("compare")) {
//				if (!where.equals(" WHERE ")) where += " AND ";
//				if (filt.containsKey("value")) {
//					if (filt.get("value_place").equals("after"))
//						filter += dist + filt.get("compare") + " " + filt.get("value");
//					else
//						filter += filt.get("value") + " " + filt.get("compare") + " " + dist;
//				}
//			}
//		}
		
		
		String compare = "", function = "", type = "";
		Set<String> binds = new HashSet<String>();
		List<String> filters = new ArrayList<String>();
		int i = 0;
		while (i < fv.size()) {
			if (fv.get(i).matches("[0-9]+")) {
				logger.info(fv.get(i));
				dist += fv.get(i);
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
			else if (fv.get(i).matches("[!<=>]+")) {
				logger.info(fv.get(i));
				compare = fv.get(i);
				i++;
			}
			else {
				logger.info(fv.get(i));
				function = fv.get(i);
				if (function.equals("distance")) dist += "ST_Distance(";
				type = fv.get(i+3);
				if (type.equals("metre")) dist += "ST_GeographyFromText(ST_AsText(";
				if (asToVar.containsKey(fv.get(i+1))) dist += asToVar.get(fv.get(i+1));
				else {
					asToVar.put(fv.get(i+1), "t"+(triples.size()+1)+".geom");
					dist += asToVar.get(fv.get(i+1));
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (type.equals("metre")) dist += ")), ST_GeographyFromText(ST_AsText(";
				else if (type.equals("degree")) dist += ", ";
				if (asToVar.containsKey(fv.get(i+2))) dist += asToVar.get(fv.get(i+2));
				else {
					asToVar.put(fv.get(i+2), "t"+(triples.size()+1)+".geom");
					dist += asToVar.get(fv.get(i+2));
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (type.equals("metre")) dist += "))";
				dist += ") ";
				
				/* Gathering binds */
				for (int b = 0; b < bv.size(); b += 5) {
					if (fv.get(i).equals(bv.get(b+1)) && fv.get(i+1).equals(bv.get(b+2)) && fv.get(i+2).equals(bv.get(b+3)) && fv.get(i+3).equals(bv.get(b+4))) {
						if (!dist.contains(compare)) binds.add(dist + "AS " + bv.get(b));
						else binds.add(dist.substring(dist.lastIndexOf(compare) + 2) + "AS " + bv.get(b));
						break;
					}
				}
				
				/* Gathering filters */
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
		
		logger.info("binds:: {}", binds);
		logger.info("filters:: {}", filters);
		
		for (String b : binds) {
			if (!select.equals("SELECT ")) select += ", ";
			select += b;
		}
		
		for (String f : filters) {
			if (!where.equals(" WHERE ")) where += " AND ";
			where += f;
		}
		
//		if (from.equals(" FROM ")) {
//			sqlQuery = select + bind + from + "lucas t1 UNION " 
//					+ select + bind + from + "invekos t1;";
//		}
		if (from.equals(" FROM ")) {
			String from1 = from, from2 = from, from3 = from, from4 = from;
			logger.info("vars size: {}", vars.size());
			logger.info("triples size: {}", triples.size());
			logger.info("asToVar size: {}", asToVar.size());
			if (triples.size() == 3 && triples.size() == 6) {
				from1 = from + "lucas t1";
				from2 = from + "invekos t1";
				if (where.equals(" WHERE ")) where = "";
				sqlQuery = select + bind + from1 + where + " UNION " 
						+ select + bind + from2 + where + ";";
			}
			else if (triples.size() == 6) {
				from1 = from + "lucas t1, lucas t4";
				from2 = from + "lucas t1, invekos t4";
				from3 = from + "invekos t1, lucas t4";
				from4 = from + "invekos t1, invekos t4";
				if (where.equals(" WHERE ")) where = "";
				sqlQuery = select + bind + from1 + where + " UNION " 
						+ select + bind + from2 + where + " UNION " 
						+ select + bind + from3 + where + " UNION " 
						+ select + bind + from4 + where + ";";
			}
			
//			sqlQuery = select + bind + from + "lucas t1 UNION " 
//					+ select + bind + from + "invekos t1;";
		}
		else {
			
			logger.info("vars size: {}", vars.size());
			logger.info("triples size: {}", triples.size());
			
			if (vars.size() == triples.size() / 3 && !triples.contains(null))
				sqlQuery = select + bind + from + where + filter + ";";
			else {
				String lucas = "", invekos = "";
				place = 1;
				while (place < triples.size()) {
					if (!from.contains("t" + place)) {
						lucas += ", lucas t" + place;
						invekos += ", invekos t" + place;
					}
					place += 3;
				}
				sqlQuery = select + bind + from + lucas + where + filter + " UNION " 
						+ select + bind + from + invekos + where + filter + ";";
			}
		}
		
		logger.info("asToVar:: {}", asToVar.toString());
		logger.info("bind:: {}", bind);
		logger.info("filter:: {}", filter);
		logger.info("sqlQuery:: {}", sqlQuery);
		return sqlQuery;
	}
	
	
	public static String buildSQLQueryUnion(TupleExpr expr, Set<String> freeVars, List<String> tables, List<BindingSet> bindings, Collection<String> relevantBindingNames) {
		Pair<String, String> tableAndGid = null;
		
		Map<String,String> filterVars = computeFunctionCallVars(expr);
		List<String> triples = computeTriples(expr);
		
		List<String> fv = computeFilterVars(expr);
		logger.info("filter variables: {} " ,fv);
		logger.info("computeFilterVars!!!! DONE");
		
		List<String> bv = computeBindVars(expr);
		logger.info("bind variables: {} " ,bv);
		logger.info("computeBindVars!!!! DONE");
		
		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
			logger.error("More than one triples with two free variables.");
			throw new QueryEvaluationException();
		}
		
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
		
		if (triples.isEmpty())
			return null;
		
		Map<String,String> asToVar = new HashMap<String, String>();
		logger.info("triples: {}", triples.toString());
		logger.info("filterVars: {}", filterVars.toString());
		
//		Set<String> bindingNames = bindings.getBindingNames();
//		for (Object binding: bindingNames) {
//			logger.info("bindings: {} - {}", binding, bindings.getValue((String)binding));
//			if (triples.contains(binding.toString())) {
//				int index = triples.indexOf(binding);
//				triples.remove(index);
//				triples.add(index, bindings.getValue((String)binding).toString());
//			}
////			asToVar.put((String) binding, bindings.getValue((String)binding).toString().replace("\"", "\'"));
//		}
		
		String select = "SELECT ", where = " WHERE ", from = " FROM ", geom = "", binding_var = "";
		int binding_place = -1;
		int place = 0;
		
		for (String part : triples) {
			place++;
			if (part.contains("#")) {	//predicate
				if (part.substring(part.lastIndexOf("#") + 1).equals("asWKT")) {
					geom = "t" + (place-1) + ".geom";
				}
				else {
					logger.error("No \"asWKT\" predicate.");
				}
			}
			else {			
				if (place % 3 != 0) {	//subject
					if (freeVars.contains(part)) {
						if (!select.equals("SELECT ")) select += ", ";
						select += "t"+ place + ".gid AS " + part;
						asToVar.put(part, "t"+ place + ".gid");
						tables.add("?");
					}
					else if (bindings.get(0).hasBinding(part)) {
						tableAndGid = subDecompose(bindings.get(0).getBinding(part).getValue().toString());
						if (!from.equals(" FROM ")) from += ", ";
						from += tableAndGid.getLeft() + " t" + place;
//						if (!where.equals(" WHERE ")) where += " AND ";
//						where_bind += "t" + place + ".gid = " + tableAndGid.getRight();
						binding_var = part;
						binding_place = place;
//						bindings.remove(0);
					}
					else {				
						tableAndGid = subDecompose(part);
						if (!from.equals(" FROM ")) from += ", ";
						from += tableAndGid.getLeft() + " t" + place;
						if (!where.equals(" WHERE ")) where += " AND ";
						where += "t" + place + ".gid = " + tableAndGid.getRight();
					}
				}
				else {					//object
					if (freeVars.contains(part)) {
						if (!select.equals("SELECT ")) select += ", ";
						select += "ST_AsText(" + geom + ") AS " + part;
						asToVar.put(part, geom);
						tables.add("-");
					}
					else {
						if (!where.equals(" WHERE ")) where += " AND ";
						where += "ST_Equals(t" + place 
								+ ".geom, ST_ST_GeomFromText('" + part + "',4326))" ;
						if (tables.get(place/3 - 1).equals("?"))
							tables.remove(place/3 - 1);
						if (part.contains("POINT")) {
							tables.add("lucas");
						}
						else if (part.contains("MULTIPOLYGON")) {
							tables.add("invekos");
						}
					}
					
				}
			}
		}
		
		logger.info("select:: {}", select);
		logger.info("from:: {}", from);
		logger.info("where:: {}", where);
		logger.info("tables:: {}", tables.toString());
		
		
		String sqlQuery = "";
//		if (freeVars.size() - 1 > triples.size() / 3)  {	//throw exception ????
//			logger.error("More than one triples with two free variables.");
//			throw new QueryEvaluationException();
//		}
		
//		String dist = "", filter = "", bind = "";
//		if (!filterVars.isEmpty()) {
//			if (filterVars.get("function").equals("distance"))
//				dist += "ST_Distance(";
//			if (filterVars.get("type").equals("metre")) {
//				dist += "ST_GeographyFromText(ST_AsText("
//						+ asToVar.get(filterVars.get("var"))
//						+ ")), ST_GeographyFromText(ST_AsText("
//						+ asToVar.get(filterVars.get("var2"))
//						+ "))";
//			}
//			else if (filterVars.get("type").equals("degree")) {
//				dist += asToVar.get(filterVars.get("var")) 
//						+ ", " + asToVar.get(filterVars.get("var2"));
//			}
//			dist += ") ";
//			if (filterVars.containsKey("signature")) {
//				if (!select.equals("SELECT ")) select += ", ";
//				bind += dist + "AS " + filterVars.get("signature");
//			}
//			if (filterVars.containsKey("compare")) {
//				if (!where.equals(" WHERE ")) where += " AND ";
//				filter += dist + filterVars.get("compare") + " " + filterVars.get("value");
//			}
//		}
		
		String dist = "", filter = "", bind = "";
		String compare = "", function = "", type = "";
		Set<String> binds = new HashSet<String>();
		List<String> filters = new ArrayList<String>();
		int i = 0;
		while (i < fv.size()) {
			if (fv.get(i).matches("[0-9]+")) {
				logger.info(fv.get(i));
				dist += fv.get(i);
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
			else if (fv.get(i).matches("[!<=>]+")) {
				logger.info(fv.get(i));
				compare = fv.get(i);
				i++;
			}
			else {
				logger.info(fv.get(i));
				function = fv.get(i);
				if (function.equals("distance")) dist += "ST_Distance(";
				type = fv.get(i+3);
				if (type.equals("metre")) dist += "ST_GeographyFromText(ST_AsText(";
				if (asToVar.containsKey(fv.get(i+1))) dist += asToVar.get(fv.get(i+1));
				else {
					asToVar.put(fv.get(i+1), "t"+(triples.size()+1)+".geom");
					dist += asToVar.get(fv.get(i+1));
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (type.equals("metre")) dist += ")), ST_GeographyFromText(ST_AsText(";
				else if (type.equals("degree")) dist += ", ";
				if (asToVar.containsKey(fv.get(i+2))) dist += asToVar.get(fv.get(i+2));
				else {
					asToVar.put(fv.get(i+2), "t"+(triples.size()+1)+".geom");
					dist += asToVar.get(fv.get(i+2));
					triples.add(null); triples.add(null); triples.add(null);
				}
				if (type.equals("metre")) dist += "))";
				dist += ") ";
				
				/* Gathering binds */
				for (int b = 0; b < bv.size(); b += 5) {
					if (fv.get(i).equals(bv.get(b+1)) && fv.get(i+1).equals(bv.get(b+2)) && fv.get(i+2).equals(bv.get(b+3)) && fv.get(i+3).equals(bv.get(b+4))) {
						if (!dist.contains(compare)) binds.add(dist + "AS " + bv.get(b));
						else binds.add(dist.substring(dist.lastIndexOf(compare) + 2) + "AS " + bv.get(b));
						break;
					}
				}
				
				/* Gathering filters */
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
		
		logger.info("binds:: {}", binds);
		logger.info("filters:: {}", filters);
		
		for (String b : binds) {
			if (!select.equals("SELECT ")) select += ", ";
			select += b;
		}
		
		for (String f : filters) {
			if (!where.equals(" WHERE ")) where += " AND ";
			where += f;
		}
		
		
		
		
//		if (from.equals(" FROM ")) {
//			sqlQuery = select + bind + from + "lucas t1 UNION " 
//					+ select + bind + from + "invekos t1;";
//		}
		if (from.equals(" FROM ")) {
			String from1 = from, from2 = from, from3 = from, from4 = from;
			logger.info("vars size: {}", freeVars.size());
			logger.info("triples size: {}", triples.size());
			logger.info("asToVar size: {}", asToVar.size());
			if (triples.size() == 3) {
				from1 = from + "lucas t1";
				from2 = from + "invekos t1";
				if (where.equals(" WHERE ")) where = "";
				sqlQuery = select + bind + from1 + where + " UNION " 
						+ select + bind + from2 + where + ";";
			}
			else if (triples.size() == 6) {
				from1 = from + "lucas t1, lucas t4";
				from2 = from + "lucas t1, invekos t4";
				from3 = from + "invekos t1, lucas t4";
				from4 = from + "invekos t1, invekos t4";
				if (where.equals(" WHERE ")) where = "";
				sqlQuery = select + bind + from1 + where + " UNION " 
						+ select + bind + from2 + where + " UNION " 
						+ select + bind + from3 + where + " UNION " 
						+ select + bind + from4 + where + ";";
			}
			
//			sqlQuery = select + bind + from + "lucas t1 UNION " 
//					+ select + bind + from + "invekos t1;";
		}
		else {
			
			logger.info("vars size: {}", freeVars.size());
			logger.info("triples size: {}", triples.size());
			
			String where_bind = "";
			if (!where.equals(" WHERE ")) where += " AND ";
			
			logger.info("now sqlQuery: {}", sqlQuery);
			
			for (BindingSet b : bindings) {
				if (!sqlQuery.equals(""))
					sqlQuery += " UNION ";
				
				tableAndGid = subDecompose(b.getBinding(binding_var).getValue().toString());
				where_bind = "t" + binding_place + ".gid = " + tableAndGid.getRight();	
				
				if (freeVars.size() == triples.size() / 3)
					sqlQuery += select + bind + from + where + where_bind + filter;
				else {
					String lucas = "", invekos = "";
					place = 1;
					while (place < triples.size()) {
						if (!from.contains("t" + place)) {
							lucas += ", lucas t" + place;
							invekos += ", invekos t" + place;
						}
						place += 3;
					}
					sqlQuery += select + bind + from + lucas + where + where_bind + filter + " UNION " 
							+ select + bind + from + invekos + where + where_bind + filter;
				}
			}
			
			sqlQuery += ";";
			
		}
		
		logger.info("asToVar:: {}", asToVar.toString());
		logger.info("bind:: {}", bind);
		logger.info("filter:: {}", filter);
		logger.info("sqlQuery:: {}", sqlQuery);
		return sqlQuery;
	}
	
	protected static Map<String,String> computeFunctionCallVars(TupleExpr serviceExpression) {
		logger.info("computeFunctionCallVars!!!!");
		final Map<String,String> res = new HashMap<String,String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(FunctionCall node) throws RuntimeException {
//				logger.info("!!! filter FunctionCall nodesignature: {}", node.getSignature());
//				logger.info("!!! filter FunctionCall nodeparentclass: {}", node.getParentNode().getClass());
				String function = node.getSignature();
				String signature = node.getParentNode().getSignature();
				String parClass = node.getParentNode().getClass().toString();
				logger.info("!!! function and compare: {} {}", function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")), signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));
				res.put("function", function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
//				res.put("parClass", parClass.substring(parClass.lastIndexOf(".") + 1));
				if (parClass.contains("Compare"))
					res.put("compare", signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));
				else if (parClass.contains("Extension"))
					res.put("signature", signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));
//				res.add(function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
//				res.add(operator.substring(operator.lastIndexOf("(") + 1, operator.lastIndexOf(")")));
				node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
					
					@Override
					public void meet(Var node) throws RuntimeException {
						logger.info("!!! var: {}", node.getName());
						if (res.containsKey("var"))
							res.put("var2", node.getName());
						else
							res.put("var", node.getName());
//						res.add(node.getName());
					}
					
					@Override
					public void meet(ValueConstant node) throws RuntimeException {
//						logger.info("!!! filter FunctionCall ValueConstant nodevalue: {}", node.getValue());
						String type = node.getValue().toString();
//						res.add(type.substring(type.lastIndexOf("/") + 1));
						res.put("type", type.substring(type.lastIndexOf("/") + 1));
						logger.info("!!! type: {}", type.substring(type.lastIndexOf("/") + 1));
					}
				});
			
			}
			
			@Override
			public void meet(ValueConstant node) throws RuntimeException {
//				logger.info("!!! filter ValueConstant nodevalue: {}", node.getValue());
				String value = node.getValue().toString();
//				res.add(value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
				res.put("value", value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
				logger.info("!!! value: {}", value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
				if (res.containsKey("compare")) res.put("value_place", "after");
				else res.put("value_place", "before");
			}
			
		});
		return res;
	}
	
	
	protected static List<String> computeBindVars(TupleExpr serviceExpression) {
		logger.info("computeBindVars!!!!");
		final List<String> res = new ArrayList<String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(ExtensionElem node) throws RuntimeException {
				String signature = node.getSignature();
//				logger.info("!!! signature: {}", signature);
				res.add(signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));

				node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
					
					@Override
					public void meet(FunctionCall node) throws RuntimeException {
						String function = node.getSignature();
//						logger.info("!!! function: {}", function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
						res.add(function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
						
						node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
							@Override
							public void meet(Var node) throws RuntimeException {
//								logger.info("!!! var: {}", node.getName());
								res.add(node.getName());
							}
							
							@Override
							public void meet(ValueConstant node) throws RuntimeException {
								String type = node.getValue().toString();
//								logger.info("!!! type: {}", type.substring(type.lastIndexOf("/") + 1));
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
		logger.info("computeFilterVars!!!!");
		final List<String> res = new ArrayList<String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(Compare node) throws RuntimeException {
				String signature = node.getSignature();
				res.add(signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));
//				logger.info("!!! signature: {}", signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));
				
				node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
					
					@Override
					public void meet(FunctionCall node) throws RuntimeException {
						String function = node.getSignature();
						res.add(function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
//						logger.info("!!! function and compare: {} {}", function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")));
						
						node.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
							@Override
							public void meet(Var node) throws RuntimeException {
//								logger.info("!!! var: {}", node.getName());
								res.add(node.getName());
							}
							
							@Override
							public void meet(ValueConstant node) throws RuntimeException {
								String type = node.getValue().toString();
//								logger.info("!!!2  type: {}", type.substring(type.lastIndexOf("/") + 1));
								res.add(type.substring(type.lastIndexOf("/") + 1));
							}
						});
					}
					
					@Override
					public void meet(ValueConstant node) throws RuntimeException {
						String value = node.getValue().toString();
//						logger.info("!!! value: {}", value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
						res.add(value.substring(value.indexOf("\"") + 1, value.lastIndexOf("\"")));
					}
				});
			}
		});
		return res;
	}
	
	
	protected static List<String> computeTriples(TupleExpr serviceExpression) {
		final List<String> res = new ArrayList<String>();
//		final int nodes = 0;
//		logger.info("serviceExpression: {} ", serviceExpression.toString());
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
		
			@Override
			public void meet(Var node)
			throws RuntimeException {
//				logger.info("triples: node: {} ", node.toString());
				// take only real vars, i.e. ignore blank nodes
				if (node.getParentNode().getClass().toString().contains("StatementPattern")) {
					if (!node.hasValue() && !node.isAnonymous())
						res.add(node.getName());
					else 
						res.add(node.getValue().toString());
				}
			}
			// TODO maybe stop tree traversal in nested SERVICE?
			// TODO special case handling for BIND
		});
		logger.info(res.toString());
		return res;
	}
	
//	protected Set<String> computeSubAndPred(TupleExpr serviceExpression) {
//		final Set<String> res = new HashSet<String>();
//		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
//		
//			@Override
//			public void meet(Var node)
//			throws RuntimeException {
////				logger.info("computeSubAndPred!!!");
////				logger.info(node.toString());
//				if (node.hasValue() && node.isAnonymous()) {
////					logger.info("inside if");
////					logger.info("name:");
////					logger.info(node.getName());
////					logger.info("value:");
////					logger.info(node.getValue().toString());
//					res.add(node.getValue().toString());
//				} 
//			}
//		});
//		return res;
//	}
	
	protected static Pair<String, String> subDecompose(String subject) {
		int gidStart = subject.lastIndexOf("/") + 1;
		String gid = subject.substring(gidStart);
//		logger.info("gid:");
//		logger.info(gid);
//		int tableStart = subject.substring(subject.lastIndexOf(".")).lastIndexOf("/");
		int lastDot = subject.lastIndexOf(".");
//		logger.info(subject.substring(lastDot));
		int tableStart = lastDot + subject.substring(lastDot).indexOf("/") + 1;
//		logger.info(subject.substring(tableStart));
		int tableEnd = tableStart + subject.substring(tableStart).indexOf("/");
//		logger.info(subject.substring(tableEnd));
//		logger.info("done!!");
		
//		Integer tableEnd = subject.substring(0, gidStart - 1).lastIndexOf("/");
//		logger.info(subject.substring(0, gidStart - 1));
//		Integer tableStart = subject.substring(0, tableEnd).lastIndexOf("/") + 1;
//		logger.info(subject.substring(0, tableEnd - 1));
		String table = subject.substring(tableStart, tableEnd);
//		logger.info("table:");
//		logger.info(table);
		return Pair.of(table, gid);
	}
	
	protected String predDecompose(String predicate, int place) {
		int columnStart = predicate.lastIndexOf("#") + 1;
//		logger.info("columnStart:: ");
//		logger.info(predicate.substring(columnStart));
		if (predicate.substring(columnStart).equals("asWKT"))
			return "ST_AsText(t" + place + ".geom)";
		else {
			logger.error("No \"asWKT\" predicate.");
			throw new QueryEvaluationException();
		}
	}

}
