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
import org.semagrow.evaluation.util.BindingSetUtil;
import org.semagrow.geospatial.execution.BBoxDistanceOptimizer;
import org.semagrow.selector.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;

public class PostGISQueryExecutor implements QueryExecutor {
	
	private static final Logger logger = LoggerFactory.getLogger(PostGISQueryExecutor.class);
    protected BindingSetOps bindingSetOps = BindingSetOpsImpl.getInstance();
    
    private BBoxDistanceOptimizer distanceOptimizer = new BBoxDistanceOptimizer();
		
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
		
		if (bindings.size() == 0) {
            return evaluateReactorImpl((PostGISSite) site, expr, bindings);
        }
        else {
            distanceOptimizer.optimize(expr, null, bindings);
            BindingSet bindingsExt = distanceOptimizer.expandBindings(bindings);

            return evaluateReactorImpl((PostGISSite) site, expr, bindingsExt)
                    .map(b -> bindingSetOps.project(bindings.getBindingNames(), b));
        }
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
		
		if (bindingList.isEmpty()) {
            return evaluateReactorImpl((PostGISSite) site, expr, bindingList);
        }
        else {
            BindingSet template =  bindingList.get(0);
            distanceOptimizer.optimize(expr, null, template);
            List<BindingSet> bindingsListExt = distanceOptimizer.expandBindings(bindingList);

            return evaluateReactorImpl((PostGISSite) site, expr, bindingsListExt);
//                    .map(b -> bindingSetOps.project(template.getBindingNames(), b));
        }
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
		
		Set<String> freeVars = computeVars(expr);
		freeVars.removeAll(bindings.getBindingNames());
		
		if (freeVars.isEmpty()) {
			// all variables in expression are bound, switch to simple ASK query or return empty ???
			logger.error("No variables in query.");
			return Flux.empty();
		} 
		else {
			final BindingSet relevantBindings = bindingSetOps.project(computeVars(expr), bindings);
            
			List<String> tables = new ArrayList<String>();
			Map<String,String> extraBindingVars = new HashMap<String, String>();
			String dbname = site.getDatabaseName();
			String sqlQuery = PostGISQueryStringUtil.buildSQLQuery(expr, freeVars, tables, relevantBindings, extraBindingVars, dbname);

			if (sqlQuery == null) return Flux.empty();
			String endpoint = site.getEndpoint();
			String username = site.getUsername();
			String password = site.getPassword();
			logger.info("Sending SQL query [{}] to {} \n\t\t with {}", sqlQuery, endpoint, relevantBindings);
			
//			PostGISClient client = PostGISClient.getInstance(endpoint, username, password);
//			Stream<Record> rs = client.execute(sqlQuery);
			
			PostGISContextProvider client = new PostGISContextProvider(endpoint, username, password);
			Stream<Record> rs = client.execute(sqlQuery);
			
			return Flux.fromStream(rs.map(r -> {
				try {
					return BindingSetOpsImpl.transform(r, dbname, extraBindingVars);
				} catch (SQLException e) {
					e.printStackTrace();
					throw new QueryEvaluationException();
				}
			}));
		}
	}
	
	/**
     * Evaluation of a remote query to a specified endpoint, given a list of bindings
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindingsList a list of bindings
     * @return Stream with the evaluation results
     * @throws QueryEvaluationException
     */
	public Flux<BindingSet>
		evaluateReactorImpl(final PostGISSite site, final TupleExpr expr, List<BindingSet> bindingsList)
				throws QueryEvaluationException {
		
		if (bindingsList.size() == 1)
            return evaluateReactorImpl(site, expr, bindingsList.get(0));
	
		Flux<BindingSet> result = null;
		
		Set<String> exprVars = computeVars(expr);

        Collection<String> relevantBindingNames = Collections.emptySet();

        if (!bindingsList.isEmpty())
        	relevantBindingNames = BindingSetUtil.projectNames(exprVars, bindingsList.get(0));
        
        Set<String> freeVars = computeVars(expr);	// freeVars = exprVars
        freeVars.removeAll(relevantBindingNames);
        
        if (freeVars.isEmpty()) {
			logger.error("No variables in query.");
		}
		else {
        
	        List<String> tables = new ArrayList<String>();
	        Map<String,String> extraBindingVars = new HashMap<String, String>();
	        String dbname = site.getDatabaseName();
	        
	        String sqlQuery = PostGISQueryStringUtil.buildSQLQueryUnion(expr, freeVars, tables, bindingsList, relevantBindingNames, extraBindingVars, dbname);
	        
	        if (sqlQuery == null) return Flux.empty();
			String endpoint = site.getEndpoint();
			String username = site.getUsername();
			String password = site.getPassword();
			
			logger.info("Sending SQL query [{}] to {} \n\t\t with {}", sqlQuery, endpoint, bindingsList);
						
//			PostGISClient client = PostGISClient.getInstance(endpoint, username, password);
//			Stream<Record> rs = client.execute(sqlQuery);
			
			PostGISContextProvider client = new PostGISContextProvider(endpoint, username, password);
			Stream<Record> rs = client.execute(sqlQuery);
			
			return Flux.fromStream(rs.map(r -> {
				try {
					return BindingSetOpsImpl.transform(r, dbname, extraBindingVars);
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
