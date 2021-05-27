package org.semagrow.connector.postgis.execution;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.jooq.Record;
import org.reactivestreams.Publisher;
import org.semagrow.connector.postgis.PostGISSite;
import org.semagrow.connector.postgis.util.BindingSetOpsImpl;
import org.semagrow.connector.postgis.util.PostGISQueryStringUtil;
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
	
	private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);
    protected BindingSetOps bindingSetOps = SimpleBindingSetOps.getInstance();
		
	public PostGISQueryExecutor() {
		
	}
	
	/**
     * Evaluation of a remote query to a specified endpoint, given a binding.
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindings a list of bindings
     * @return A publisher who publishes the evaluation results
     * @throws QueryEvaluationException
     */
	public Publisher<BindingSet> evaluate(final Site site, final TupleExpr expr, final BindingSet bindings)
			throws QueryEvaluationException {
		return evaluateReactorImpl((PostGISSite)site, expr, bindings);
	}
	
	/**
     * Evaluation of a remote query to a specified endpoint, given a list of bindings.
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindingList a list of bindings
     * @return A publisher who publishes the evaluation results
     * @throws QueryEvaluationException
     */
	public Publisher<BindingSet> evaluate(final Site site, final TupleExpr expr, final List<BindingSet> bindingList)
			throws QueryEvaluationException {
		return evaluateReactorImpl((PostGISSite)site, expr, bindingList);
	}
	
	/**
     * Evaluation of a remote query to a specified endpoint, given a binding
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindings the binding
     * @return Stream with the evaluation results
     * @throws QueryEvaluationException
     */
	public Flux<BindingSet>
		evaluateReactorImpl(final PostGISSite site, final TupleExpr expr, final BindingSet bindings)
				throws QueryEvaluationException {
		
		Flux<BindingSet> result = null;
		
		Set<String> freeVars = computeVars(expr);
		freeVars.removeAll(bindings.getBindingNames());
		
		logger.debug("evaluateReactorImpl!!!");
		logger.debug("expr: {}" , expr);
		logger.debug("endpoint: {}", site.toString());
		logger.debug("bindings: {}", bindings.toString());
		logger.debug("freeVars: {}", freeVars.toString());
		
		if (freeVars.isEmpty()) {
			// all variables in expression are bound, switch to simple ASK query or return empty ???
			logger.error("No variables in query.");
			result = Flux.empty();
		} 
		else {
			final BindingSet relevantBindings = bindingSetOps.project(computeVars(expr), bindings);
            logger.debug("relevantBindings:: {}", relevantBindings.toString());
			
			List<String> tables = new ArrayList<String>();
			String sqlQuery = PostGISQueryStringUtil.buildSQLQuery(expr, freeVars, tables, relevantBindings);
			
			if (sqlQuery == null) return Flux.empty();
			String endpoint = site.getEndpoint();
			String username = site.getUsername();
			String password = site.getPassword();
			logger.info("Sending SQL query [{}] to [{}]", sqlQuery, endpoint);
			
			PostGISClient client = PostGISClient.getInstance(endpoint, username, password);
			Stream<Record> rs = client.execute(sqlQuery);
			return Flux.fromStream(rs.map(r -> {
				try {
					return BindingSetOpsImpl.transform(r);
				} catch (SQLException e) {
					e.printStackTrace();
					throw new QueryEvaluationException();
				}
			}));
		}
		
		return result;		
	}
	
	/**
     * Evaluation of a remote query to a specified endpoint, given a list of bindings
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindings a list of bindings
     * @return Stream with the evaluation results
     * @throws QueryEvaluationException
     */
	public Flux<BindingSet>
		evaluateReactorImpl(final PostGISSite site, final TupleExpr expr, List<BindingSet> bindings)
				throws QueryEvaluationException {
		
		if (bindings.size() == 1)
            return evaluateReactorImpl(site, expr, bindings.get(0));
	
		Flux<BindingSet> result = null;
		
		Set<String> exprVars = computeVars(expr);

        Collection<String> relevantBindingNames = Collections.emptySet();

        if (!bindings.isEmpty())
        	relevantBindingNames = BindingSetUtil.projectNames(exprVars, bindings.get(0));
        
        logger.debug("evaluateReactorImpl 2!!!");
		logger.debug("expr: {}" , expr);
		logger.debug("bindings: {}", bindings.toString());
        logger.debug("bindings.get(0): {}", bindings.get(0));
        logger.debug("relevantBindingNames: {}", relevantBindingNames);
        
        Set<String> freeVars = computeVars(expr);	// freeVars = exprVars
        freeVars.removeAll(relevantBindingNames);
        
        if (freeVars.isEmpty()) {
			logger.error("No variables in query.");
		}
		else {
        
	        List<String> tables = new ArrayList<String>();
	        String sqlQuery = PostGISQueryStringUtil.buildSQLQueryUnion(expr, freeVars, tables, bindings, relevantBindingNames);
			
	        if (sqlQuery == null) return Flux.empty();
			String endpoint = site.getEndpoint();
			String username = site.getUsername();
			String password = site.getPassword();
			logger.info("Sending SQL query [{}] to [{}]", sqlQuery, endpoint);
			
			PostGISClient client = PostGISClient.getInstance(endpoint, username, password);
			Stream<Record> rs = client.execute(sqlQuery);
			return Flux.fromStream(rs.map(r -> {
				try {
					return BindingSetOpsImpl.transform(r);
				} catch (SQLException e) {
					e.printStackTrace();
					throw new QueryEvaluationException();
				}
			}));
		}
        
        return result;
	}
	
	
	protected Set<String> computeVars(TupleExpr serviceExpression) {
		final Set<String> res = new HashSet<String>();
		serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {
		
			@Override
			public void meet(Var node) throws RuntimeException {
				// take only real vars, i.e. ignore blank nodes
				if (!node.hasValue() && !node.isAnonymous())
					res.add(node.getName());
			}
			// TODO maybe stop tree traversal in nested SERVICE?
			// TODO special case handling for BIND
		});
		return res;
	}
	
}
