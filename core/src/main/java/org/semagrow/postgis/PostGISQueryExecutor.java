package org.semagrow.postgis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.reactivestreams.Publisher;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
import org.semagrow.selector.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;

import java.net.MalformedURLException;
import java.net.URL;

public class PostGISQueryExecutor implements QueryExecutor {
	
    private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);

    protected BindingSetOps bindingSetOps = new BindingSetOpsImpl();

    public PostGISQueryExecutor() {
    	logger.info("PostGISQueryExecutor!!!");
    }
    
	public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {
		logger.info("evaluate!!!");
//		//PostGISClient client = PostGISClient.getInstance("jdbc:postgresql://localhost:5432/semdb", "postgres", "postgres");
//		if (bindings.size() == 0) {
//			return sendSqlQuery(endpoint, expr, Collections.emptyList());
//        }
//
//        return evaluate(endpoint, expr, Collections.singletonList(bindings));
		URL myURL = null;
		try {
			myURL = new URL("http://localhost:30400");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PostGISSite site = new PostGISSite(myURL);
		return evaluateReactorImpl(site, expr, bindings);
//		return evaluateReactorImpl((PostGISSite)endpoint, expr, bindings);
    }
	
	public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final List<BindingSet> bindingList)
            throws QueryEvaluationException {
    	logger.info("evaluate 2!!!");
//    	if (bindingList.isEmpty()) {
//    		return sendSqlQuery(endpoint, expr, Collections.emptyList());
//        }
//    	
//    	logger.info("bindingList not empty: not yet created");
    	
    	return evaluateReactorImpl(endpoint, expr, bindingList);
//    	return evaluateReactorImpl((PostGISSite)endpoint, expr, bindingList);
//		return null;
    }
    
    public Flux<BindingSet>
    evaluateReactorImpl(final PostGISSite endpoint, final TupleExpr expr, final BindingSet bindings)
        throws QueryEvaluationException {
    	Flux<BindingSet> result = null;
    	logger.info("evaluateReactorImpl!!!");
    	logger.info(endpoint.toString());
    	
    	Set<String> freeVars = computeVars(expr);

        freeVars.removeAll(bindings.getBindingNames());
        
        if (freeVars.isEmpty()) {
        	logger.error("No variables in query.");
        } 
        else {

//            final BindingSet relevantBindings = bindingSetOps.project(computeVars(expr), bindings);
//            logger.info("relevantBindings::");
//            logger.info(relevantBindings.toString());
            
            Set<String> subAndPred = computeSubAndPred(expr);
//            logger.info("subAndPred:: ");
//            logger.info(subAndPred.toString());
            String column = null;
            Pair<String, String> tableAndGid = null;
            for (String temp : subAndPred) {
            	if (temp.contains("#")) {
//            		logger.info("predicate:: ");
//            		logger.info(temp);
            		column = predDecompose(temp);
            	}
            	else {
//            		logger.info("subject:: ");
//            		logger.info(temp);
            		tableAndGid = subDecompose(temp);
//            		logger.info("table and gid:: ");
//            		logger.info(tableAndGid.getLeft());
//            		logger.info(tableAndGid.getRight());
            	}
        	}
            
            if (column == null)
            	logger.error("No \"asWKT\" predicate.");
            	
            String sqlQuery = "select " + column + 
            		" from " + tableAndGid.getLeft() + 
            		" where gid = " + tableAndGid.getRight() + ";";
            
            logger.info("sqlQuery:: ");
    		logger.info(sqlQuery);
            
//            String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);
////            logger.info(expr.toString());
////            logger.info(freeVars.toString());
//            logger.info("sparqlQuery::");
//            logger.info(sparqlQuery);
//            result = sendTupleQuery(endpoint.getURL(), sparqlQuery, relevantBindings, expr)
//                    .map(b -> bindingSetOps.merge(bindings, b));
//            logger.info("result::");
//            logger.info(result.toString());
//            logger.info("done");
        }
    	
    	
    	return result;
    }
    
    public Flux<BindingSet>
    evaluateReactorImpl(final Site endpoint, final TupleExpr expr, List<BindingSet> bindings)
        throws QueryEvaluationException {
    	Flux<BindingSet> result = null;
    	logger.info("evaluateReactorImpl 2!!!");
    	return result;
    }
	
//    private Publisher<BindingSet> sendSqlQuery(Site site, TupleExpr expr, List<BindingSet> bindingsList) {
//    	logger.info("sendSqlQuery!!!");
//    	PostGISClient client = PostGISClient.getInstance("jdbc:postgresql://localhost:5432/semdb", "postgres", "postgres");
//    	return null;
//    }
    
    protected Set<String> computeVars(TupleExpr serviceExpression) {
        final Set<String> res = new HashSet<String>();
        serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(Var node)
                    throws RuntimeException {
                // take only real vars, i.e. ignore blank nodes
                if (!node.hasValue() && !node.isAnonymous())
                    res.add(node.getName());
            }
            // TODO maybe stop tree traversal in nested SERVICE?
            // TODO special case handling for BIND
        });
        return res;
    }
    
    protected Set<String> computeSubAndPred(TupleExpr serviceExpression) {
        final Set<String> res = new HashSet<String>();
        serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(Var node)
                    throws RuntimeException {
                // take only real vars, i.e. ignore blank nodes
//            	logger.info("computeSubAndPred!!!");
//            	logger.info(node.toString());
                if (node.hasValue() && node.isAnonymous()) {
//                	logger.info("inside if");
//                	logger.info("name:");
//                	logger.info(node.getName());
//                	logger.info("value:");
//                	logger.info(node.getValue().toString());
                	res.add(node.getValue().toString());
                } 
            }
            // TODO maybe stop tree traversal in nested SERVICE?
            // TODO special case handling for BIND
        });
        return res;
    }
    
    protected Pair<String, String> subDecompose(String subject) {
        int gidStart = subject.lastIndexOf("/") + 1;
        String gid = subject.substring(gidStart);
//        logger.info("gid:");
//        logger.info(gid);
//        int tableStart = subject.substring(subject.lastIndexOf(".")).lastIndexOf("/");
        int lastDot = subject.lastIndexOf(".");
//        logger.info(subject.substring(lastDot));
        int tableStart = lastDot + subject.substring(lastDot).indexOf("/") + 1;
//        logger.info(subject.substring(tableStart));
        int tableEnd = tableStart + subject.substring(tableStart).indexOf("/");
//        logger.info(subject.substring(tableEnd));
//        logger.info("done!!");
        
//        Integer tableEnd = subject.substring(0, gidStart - 1).lastIndexOf("/");
//        logger.info(subject.substring(0, gidStart - 1));
//        Integer tableStart = subject.substring(0, tableEnd).lastIndexOf("/") + 1;
//        logger.info(subject.substring(0, tableEnd - 1));
        String table = subject.substring(tableStart, tableEnd);
//        logger.info("table:");
//        logger.info(table);
        return Pair.of(table, gid);
    }
    
    protected String predDecompose(String predicate) {
        int columnStart = predicate.lastIndexOf("#") + 1;
//        logger.info("columnStart:: ");
//        logger.info(predicate.substring(columnStart));
        if (predicate.substring(columnStart).equals("asWKT"))
        	return "geom";
        else 
        	return null;
    }
    
}
