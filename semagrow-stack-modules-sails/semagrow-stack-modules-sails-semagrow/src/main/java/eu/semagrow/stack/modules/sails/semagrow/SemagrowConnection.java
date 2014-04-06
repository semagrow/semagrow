package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.querydecomp.selector.VOIDLoader;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.EvaluationStrategy;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.FilterOptimizer;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.SingleSourceClusterOptimizer;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.SingleSourceProjectionOptimization;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.SingleSourcetoServiceConverter;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.StatementSourceSelection;
import eu.semagrow.stack.modules.utils.resourceselector.ResourceSelector;
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
 * Created by angel on 3/12/14.
 */
public class SemagrowConnection extends ReadonlySailConnection {

    public SemagrowConnection(SemagrowSail sail)
    {
        super(sail);
    }

    @Override
    protected void closeInternal() throws SailException {

    }

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
        QueryOptimizer optimizer = getSourceOptimizer();

        optimizer.optimize(tupleExpr,dataset,bindings);
        System.out.print(QueryModelTreePrinter.printTree(tupleExpr));

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
