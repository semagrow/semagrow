package org.semagrow.postgis;

import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.reactivestreams.Publisher;
import org.semagrow.postgis.PostGISClient;
import org.semagrow.postgis.PostGISSite;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.file.MaterializationManager;
import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
import org.semagrow.evaluation.util.SimpleBindingSetOps;
import org.semagrow.querylog.api.QueryLogHandler;
import org.semagrow.selector.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PostGISQueryExecutor implements QueryExecutor {
	
    private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);

//	private boolean rowIdOpt = false;
//    private QueryLogHandler qfrHandler;
//    private MaterializationManager mat;

    protected BindingSetOps bindingSetOps = SimpleBindingSetOps.getInstance();

//    public PostGISQueryExecutor(QueryLogHandler qfrHandler, MaterializationManager mat) {
    public PostGISQueryExecutor() {
    	logger.info("PostGISQueryExecutor!!!");
//    	this.qfrHandler = qfrHandler;
//        this.mat = mat;
    }
	
	public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {
		return null;
    }
	
	public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final List<BindingSet> bindingList)
            throws QueryEvaluationException {
		sendSqlQuery((PostGISSite)endpoint, expr, bindingList);
		return null;
    }
	
    private void sendSqlQuery(PostGISSite site, TupleExpr expr, List<BindingSet> bindingsList) {
    	PostGISClient client = PostGISClient.getInstance("jdbc:postgresql://localhost:5432/semdb", "postgres", "postgres");
    }
	
}

//package org.semagrow.connector.postgis;
//
//import org.semagrow.cassandra.eval.CassandraQueryExecutorImpl;
//import org.semagrow.evaluation.reactor.PostGISSite;
//import org.semagrow.evaluation.file.MaterializationManager;
//import org.semagrow.evaluation.BindingSetOps;
//import org.semagrow.evaluation.util.BindingSetUtil;
//import org.semagrow.evaluation.util.LoggingUtil;
//import org.semagrow.evaluation.util.SimpleBindingSetOps;
//import org.semagrow.evaluation.QueryExecutor;
//import org.semagrow.connector.sparql.query.render.SPARQLQueryStringUtil;
//import org.semagrow.selector.Site;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.semagrow.querylog.api.QueryLogHandler;
//import org.eclipse.rdf4j.query.*;
//import org.eclipse.rdf4j.query.algebra.TupleExpr;
//import org.eclipse.rdf4j.query.algebra.Var;
//import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
//import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
//import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
//import org.eclipse.rdf4j.repository.RepositoryConnection;
//import org.eclipse.rdf4j.repository.RepositoryException;
//import org.reactivestreams.Publisher;
//import reactor.core.publisher.Flux;
//
//import java.net.URL;
//import java.util.*;
//
//
//public class PostGISQueryExecutor implements QueryExecutor
//{
//	private boolean rowIdOpt = false;
//    private QueryLogHandler qfrHandler;
//    private MaterializationManager mat;
//
//    protected BindingSetOps bindingSetOps = SimpleBindingSetOps.getInstance();
//
//    public PostGISQueryExecutor(QueryLogHandler qfrHandler, MaterializationManager mat) {
//        this.qfrHandler = qfrHandler;
//        this.mat = mat;
//    }
//    
//    
//    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final BindingSet bindings)
//            throws QueryEvaluationException {
//        return evaluatePostGISImpl((PostGISSite)endpoint, expr, bindings);
//    }
//    
//    
//    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final List<BindingSet> bindingList)
//            throws QueryEvaluationException {
//        try {
//            return evaluatePostGISImpl((PostGISSite)endpoint, expr, bindingList);
//        }catch(QueryEvaluationException e)
//        {
//            throw e;
//        } catch (Exception e) {
//            throw new QueryEvaluationException(e);
//        }
//
//    }
//    
//    
//    public Flux<BindingSet> 
//    	evaluatePostGISImpl(final PostGISSite endpoint, final TupleExpr expr, final BindingSet bindings)
//            throws QueryEvaluationException {
////        Flux<BindingSet> result = null;
////        
////        try {
////        
////        final BindingSet relevantBindings = bindingSetOps.project(computeVars(expr), bindings);
////        
////        String sqlQuery = "select ST_AsText(geom) from lucas where gid=9";
////
////        result = sendTupleQuery(endpoint.getURL(), sqlQuery, relevantBindings, expr)
////                .map(b -> bindingSetOps.merge(bindings, b));
////        
////        } catch (QueryEvaluationException e) {
////            throw e;
////        } catch (Exception e) {
////            throw new QueryEvaluationException(e);
////        }
//    	return null;
//    }
//    
//    
//    protected Flux<BindingSet>
//    evaluatePostGISImpl(PostGISSite endpoint, TupleExpr expr, List<BindingSet> bindings)
//        throws QueryEvaluationException {
//    	
//    	return null;
//    }
//    
//    
////    protected Flux<BindingSet>
////        sendTupleQuery(URL endpoint, String sqlQuery, BindingSet bindings, TupleExpr expr)
////            throws QueryEvaluationException, MalformedQueryException, RepositoryException {
////
////        RepositoryConnection conn = getConnection(endpoint);
////        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sqlQuery);
////
////        for (Binding b : bindings)
////            query.setBinding(b.getName(), b.getValue());
////
////        LoggingUtil.logRemote(logger, conn, sqlQuery, endpoint, expr, query);
////
////        return Flux.from(new TupleQueryResultPublisher(query, sqlQuery, qfrHandler, mat, endpoint))
////                .doAfterTerminate(() -> closeQuietly(conn));
////    }
////    
////    
////    protected Set<String> computeVars(TupleExpr serviceExpression) {
////        final Set<String> res = new HashSet<String>();
////        serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
////
////            @Override
////            public void meet(Var node)
////                    throws RuntimeException {
////                // take only real vars, i.e. ignore blank nodes
////                if (!node.hasValue() && !node.isAnonymous())
////                    res.add(node.getName());
////            }
////            // TODO maybe stop tree traversal in nested SERVICE?
////            // TODO special case handling for BIND
////        });
////        return res;
////    }
//}
