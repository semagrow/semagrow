package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.querydecomp.SourceSelector;
import eu.semagrow.stack.modules.querydecomp.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.querydecomp.selector.SourceSelectorAdapter;
import eu.semagrow.stack.modules.querydecomp.selector.VOIDLoader;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CostEstimatorImpl;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.EvaluationStrategy;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.*;
import eu.semagrow.stack.modules.api.ResourceSelector;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.evaluation.util.QueryOptimizerList;
import org.openrdf.query.algebra.helpers.QueryModelTreePrinter;
import org.openrdf.sail.SailException;

/**
 * A Semagrow Readonly Connection
 * @author acharal@iit.demokritos.gr
 */
public class SemagrowConnection extends ReadonlySailConnection {

    public SemagrowConnection(SemagrowSail sail)
    {
        super(sail);
    }

    @Override
    protected void closeInternal() throws SailException {

    }

    /**
     * Evaluates a query represented as TupleExpr
     * @param tupleExpr the tuple expression to evaluate
     * @param dataset
     * @param bindings
     * @param b
     * @return the resultset as a closable iteration
     * @throws SailException
     */
    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException>
                   evaluateInternal(TupleExpr tupleExpr,
                                    Dataset dataset,
                                    BindingSet bindings,
                                    boolean b) throws SailException {
        /*
        * triggers an evaluation of a query.
        * query optimization is placed traditionally here prior evaluation.
        * then evaluation strategy is called to actually evaluate the ``optimized'' query
        * */
        //QueryOptimizer optimizer = getSourceOptimizer();
        QueryOptimizer optimizer = getOptimizer();

        optimizer.optimize(tupleExpr,dataset,bindings);
        //System.out.print(QueryModelTreePrinter.printTree(tupleExpr));

        EvaluationStrategy strategy = new EvaluationStrategy();

        try {
            return strategy.evaluate(tupleExpr,bindings);
        } catch (QueryEvaluationException e) {
            throw new SailException(e);
        }

    }

    private QueryOptimizer getSourceOptimizer() {

        VOIDLoader loader = new VOIDLoader();
        ResourceSelector selector = loader.getSelector();

        QueryOptimizerList optimizer = new QueryOptimizerList(
                new ConjunctiveConstraintSplitter(), // split conjunctive conditions to separate filters
                new CompareOptimizer(),
                new SameTermFilterOptimizer(),
                new QueryModelNormalizer(),
                new StatementSourceSelection(selector),
                new SingleSourceClusterOptimizer(),
                new FilterOptimizer(),
                new SingleSourceProjectionOptimization(),
                new SingleSourcetoServiceConverter()
        );

        return optimizer;
    }

    private QueryOptimizer getOptimizer() {
        SourceSelector selector = getSourceSelector();
        CostEstimator costEstimator = getCostEstimator();

        QueryOptimizerList optimizer = new QueryOptimizerList(
            new ConjunctiveConstraintSplitter(),
            new CompareOptimizer(),
            new SameTermFilterOptimizer(),
            new DynamicProgrammingOptimizer(costEstimator,selector),
            new SingleSourceProjectionOptimization()
        );

        return optimizer;
    }

    private SourceSelector getSourceSelector() {
        VOIDLoader loader = new VOIDLoader();
        ResourceSelector resourceSelector = loader.getSelector();
        return new SourceSelectorAdapter(resourceSelector);
    }

    private CostEstimator getCostEstimator() {
        CardinalityEstimator cardinalityEstimator = null;
        CostEstimator estimator = new CostEstimatorImpl(cardinalityEstimator);
        return estimator;
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException>
                    getContextIDsInternal() throws SailException {
        return null;
    }

    @Override
    protected CloseableIteration<? extends Statement, SailException>
                    getStatementsInternal(Resource resource, URI uri, Value value, boolean b, Resource... resources) throws SailException {
        return null;
    }

    @Override
    protected long sizeInternal(Resource... resources) throws SailException {
        return 0;
    }

    @Override
    protected void startTransactionInternal() throws SailException {

    }

    @Override
    protected void commitInternal() throws SailException {

    }

    @Override
    protected void rollbackInternal() throws SailException {

    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {
        return null;
    }

    @Override
    protected String getNamespaceInternal(String s) throws SailException {
        return null;
    }

}
