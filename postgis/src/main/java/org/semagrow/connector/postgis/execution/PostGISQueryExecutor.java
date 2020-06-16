package org.semagrow.connector.postgis.execution;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.jooq.Record;
import org.reactivestreams.Publisher;
import org.semagrow.connector.postgis.util.BindingSetOpsImpl;
import org.semagrow.connector.postgis.PostGISSite;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
import org.semagrow.evaluation.util.BindingSetUtil;
import org.semagrow.evaluation.util.SimpleBindingSetOps;
import org.semagrow.selector.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;

public class PostGISQueryExecutor implements QueryExecutor {
	
    protected BindingSetOps bindingSetOps = SimpleBindingSetOps.getInstance();
	private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);
	
//	protected BindingSetOpsImpl bindingSetOps = new BindingSetOpsImpl();
	
	public PostGISQueryExecutor() {
		logger.info("PostGISQueryExecutor!!!");
	}
	
	public Publisher<BindingSet> evaluate(final Site site, final TupleExpr expr, final BindingSet bindings)
	throws QueryEvaluationException {
		logger.info("evaluate!!!");
//		logger.info("endpoint: {}", endpoint);
//		
//		String url = "jdbc:" + endpoint.toString().substring(endpoint.toString().indexOf("/") + 2);
//		logger.info("endpoint: {}", url);
		
//		//PostGISClient client = PostGISClient.getInstance("jdbc:postgresql://localhost:5432/semdb", "postgres", "postgres");
//		if (bindings.size() == 0) {
//			return sendSqlQuery(endpoint, expr, Collections.emptyList());
//		}
//		
//		return evaluate(endpoint, expr, Collections.singletonList(bindings));
//		URL myURL = null;
//		try {
////			myURL = new URL("jdbc:postgresql://localhost/semdb");
////			myURL = new URL(endpoint);
//			myURL = new URL("http://localhost:30400");
//		} catch (MalformedURLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		PostGISSite site = new PostGISSite(myURL);
//		return evaluateReactorImpl(site, expr, bindings);
		return evaluateReactorImpl((PostGISSite)site, expr, bindings);
	}
	
	public Publisher<BindingSet> evaluate(final Site site, final TupleExpr expr, final List<BindingSet> bindingList)
	throws QueryEvaluationException {
		logger.info("evaluate 2!!!");
//		if (bindingList.isEmpty()) {
//			return sendSqlQuery(endpoint, expr, Collections.emptyList());
//		}
//		
//		logger.info("bindingList not empty: not yet created");
		
//		return evaluateReactorImpl(endpoint, expr, bindingList);
		return evaluateReactorImpl((PostGISSite)site, expr, bindingList);
//		return null;
	}
	
	public Flux<BindingSet>
		evaluateReactorImpl(final PostGISSite site, final TupleExpr expr, final BindingSet bindings)
				throws QueryEvaluationException {
		Flux<BindingSet> result = null;
		logger.info("evaluateReactorImpl!!!");
		logger.info("endpoint: {}", site.toString());
		logger.info("bindings: {}", bindings.toString());
		logger.info("!!!!!!!!!!!!expr:: {}", expr.toString());
		
		Set<String> freeVars = computeVars(expr);
		logger.info("freeVars: {}", freeVars.toString());
//		Map<String,String> filterVars = computeFilterVars(expr);
//		List<String> triples = computeTriples(expr);
//		logger.info("triples: {}", filterVars.toString());
//		logger.info("filterVars: {}", filterVars.toString());
		
		
		freeVars.removeAll(bindings.getBindingNames());
		
		logger.info("freeVars again: {}", freeVars.toString());
		logger.info("bindings: {}", bindings.toString());
		
		if (freeVars.isEmpty()) {
			logger.error("No variables in query.");
		} 
		else {
			final BindingSet relevantBindings = bindingSetOps.project(computeVars(expr), bindings);
            logger.info("relevantBindings:: {}", relevantBindings.toString());
			
			
			List<String> tables = new ArrayList<String>();
			String sqlQuery = buildSQLQuery(expr, freeVars, tables, relevantBindings);
			if (sqlQuery == null) return Flux.empty();
			String endpoint = site.getEndpoint();
			String username = site.getUsername();
			String password = site.getPassword();
			logger.info("endpoint: {}", endpoint);
			logger.info("Sending SQL query [{}] to [{}]", sqlQuery, endpoint);
			PostGISClient client = PostGISClient.getInstance(endpoint, username, password);
			Stream<Record> rs = client.execute(sqlQuery);
			return Flux.fromStream(rs.map(r -> {
				try {
					return BindingSetOpsImpl.transform(r, tables);
				} catch (SQLException e) {
					e.printStackTrace();
					throw new QueryEvaluationException();
				}
			}));
		}
		
		return result;		
	}
	
	public Flux<BindingSet>
		evaluateReactorImpl(final PostGISSite site, final TupleExpr expr, List<BindingSet> bindings)
				throws QueryEvaluationException {
		Flux<BindingSet> result = null;
		logger.info("!!!!evaluateReactorImpl 2!!!!!!");
		logger.info("bindings: {}", bindings.toString());
		if (bindings.size() == 1)
            return evaluateReactorImpl(site, expr, bindings.get(0));
	
		Set<String> exprVars = computeVars(expr);

        Collection<String> relevantBindingNames = Collections.emptySet();

        if (!bindings.isEmpty())
        	relevantBindingNames = BindingSetUtil.projectNames(exprVars, bindings.get(0));
        
        logger.info("bindings.get(0): {}", bindings.get(0));
        logger.info("relevantBindingNames: {}", relevantBindingNames);
        
        Set<String> freeVars = computeVars(expr);
        freeVars.removeAll(relevantBindingNames);
        
        if (freeVars.isEmpty()) {
			logger.error("No variables in query.");
		}
		else {
        
	        List<String> tables = new ArrayList<String>();
	        String sqlQuery = buildSQLQueryUnion(expr, freeVars, tables, bindings, relevantBindingNames);
			
	        if (sqlQuery == "") return Flux.empty();
			String endpoint = site.getEndpoint();
			String username = site.getUsername();
			String password = site.getPassword();
			logger.info("endpoint: {}", endpoint);
			logger.info("Sending SQL query [{}] to [{}]", sqlQuery, endpoint);
			PostGISClient client = PostGISClient.getInstance(endpoint, username, password);
			Stream<Record> rs = client.execute(sqlQuery);
			return Flux.fromStream(rs.map(r -> {
				try {
					return BindingSetOpsImpl.transform(r, tables);
				} catch (SQLException e) {
					e.printStackTrace();
					throw new QueryEvaluationException();
				}
			}));
		}
        
        return result;
	}
	
//	private Publisher<BindingSet> sendSqlQuery(Site site, TupleExpr expr, List<BindingSet> bindingsList) {
//		logger.info("sendSqlQuery!!!");
//		PostGISClient client = PostGISClient.getInstance("jdbc:postgresql://localhost:5432/semdb", "postgres", "postgres");
//		return null;
//	}
	
	protected String buildSQLQuery(TupleExpr expr, Set<String> vars, List<String> tables, BindingSet bindings) {
		
		Pair<String, String> tableAndGid = null;
		
		Map<String,String> filterVars = computeFilterVars(expr);
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
		logger.info("triples: {}", triples.toString());
		logger.info("filterVars: {}", filterVars.toString());
		
		Set<String> bindingNames = bindings.getBindingNames();
		for (Object binding: bindingNames) {
			logger.info("bindings: {} - {}", binding, bindings.getValue((String)binding));
			if (triples.contains(binding.toString())) {
				int index = triples.indexOf(binding);
				triples.remove(index);
				triples.add(index, bindings.getValue((String)binding).toString());
			}
		}
		
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
		if (vars.size() - 1 > triples.size() / 3)  {	//throw exception ????
			logger.error("More than one triples with two free variables.");
			throw new QueryEvaluationException();
		}
		
		String dist = "", filter = "", bind = "";
		if (!filterVars.isEmpty()) {
			if (filterVars.get("function").equals("distance"))
				dist += "ST_Distance(";
			if (filterVars.get("type").equals("metre")) {
				dist += "ST_GeographyFromText(ST_AsText("
						+ asToVar.get(filterVars.get("var"))
						+ ")), ST_GeographyFromText(ST_AsText("
						+ asToVar.get(filterVars.get("var2"))
						+ "))";
			}
			else if (filterVars.get("type").equals("degree")) {
				dist += asToVar.get(filterVars.get("var")) 
						+ ", " + asToVar.get(filterVars.get("var2"));
			}
			dist += ") ";
			if (filterVars.containsKey("signature")) {
				if (!select.equals("SELECT ")) select += ", ";
				bind += dist + "AS " + filterVars.get("signature");
			}
			if (filterVars.containsKey("compare")) {
				if (!where.equals(" WHERE ")) where += " AND ";
				filter += dist + filterVars.get("compare") + " " + filterVars.get("value");
			}
		}
		
		
		if (from.equals(" FROM ")) {	
			sqlQuery = select + bind + from + "lucas t1 UNION " 
					+ select + bind + from + "invekos t1;";
		}
		else {
			
			logger.info("vars size: {}", vars.size());
			logger.info("triples size: {}", triples.size());
			
			if (vars.size() == triples.size() / 3)
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
	
	
	protected String buildSQLQueryUnion(TupleExpr expr, Set<String> freeVars, List<String> tables, List<BindingSet> bindings, Collection<String> relevantBindingNames) {
		Pair<String, String> tableAndGid = null;
		
		Map<String,String> filterVars = computeFilterVars(expr);
		List<String> triples = computeTriples(expr);
		
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
		
		String dist = "", filter = "", bind = "";
		if (!filterVars.isEmpty()) {
			if (filterVars.get("function").equals("distance"))
				dist += "ST_Distance(";
			if (filterVars.get("type").equals("metre")) {
				dist += "ST_GeographyFromText(ST_AsText("
						+ asToVar.get(filterVars.get("var"))
						+ ")), ST_GeographyFromText(ST_AsText("
						+ asToVar.get(filterVars.get("var2"))
						+ "))";
			}
			else if (filterVars.get("type").equals("degree")) {
				dist += asToVar.get(filterVars.get("var")) 
						+ ", " + asToVar.get(filterVars.get("var2"));
			}
			dist += ") ";
			if (filterVars.containsKey("signature")) {
				if (!select.equals("SELECT ")) select += ", ";
				bind += dist + "AS " + filterVars.get("signature");
			}
			if (filterVars.containsKey("compare")) {
				if (!where.equals(" WHERE ")) where += " AND ";
				filter += dist + filterVars.get("compare") + " " + filterVars.get("value");
			}
		}
		
		
		if (from.equals(" FROM ")) {	
			sqlQuery = select + bind + from + "lucas t1 UNION " 
					+ select + bind + from + "invekos t1;";
		}
		else {
			
			logger.info("vars size: {}", freeVars.size());
			logger.info("triples size: {}", triples.size());
			
			String where_bind = "";
			if (!where.equals(" WHERE ")) where += " AND ";
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
	
	
	protected Set<String> computeVars(TupleExpr serviceExpression) {
		final Set<String> res = new HashSet<String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
		
			@Override
			public void meet(Var node)
			throws RuntimeException {
				logger.info("node: {}", node.toString());
				// take only real vars, i.e. ignore blank nodes
//				if (!filter && node.getParentNode().getClass().toString().contains("StatementPattern")) {
//					logger.info("AAAAAAAAAAAAAAAA: {}", node.getParentNode().getClass());
				if (!node.hasValue() && !node.isAnonymous())
					res.add(node.getName());
//				}
//				logger.info("AAAAAAAAAAAAAAAA: {}", node.getParentNode().getClass());
//				if (filter && node.getParentNode().getClass().toString().contains("StatementPattern"))
			}
			// TODO maybe stop tree traversal in nested SERVICE?
			// TODO special case handling for BIND
		});
		return res;
	}
	
	protected Map<String,String> computeFilterVars(TupleExpr serviceExpression) {
		logger.info("computeFilterVars!!!!");
		final Map<String,String> res = new HashMap<String,String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			
			@Override
			public void meet(FunctionCall node) throws RuntimeException {
//				logger.info("!!! filter FunctionCall nodesignature: {}", node.getSignature());
//				logger.info("!!! filter FunctionCall nodeparentclass: {}", node.getParentNode().getClass());
				String function = node.getSignature();
				String signature = node.getParentNode().getSignature();
				String parClass = node.getParentNode().getClass().toString();
				logger.info("!!! function and operator: {} {}", function.substring(function.lastIndexOf("/") + 1, function.lastIndexOf(")")), signature.substring(signature.lastIndexOf("(") + 1, signature.lastIndexOf(")")));
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
			}
			
		});
		return res;
	}
	
	protected List<String> computeTriples(TupleExpr serviceExpression) {
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
	
	protected Pair<String, String> subDecompose(String subject) {
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
