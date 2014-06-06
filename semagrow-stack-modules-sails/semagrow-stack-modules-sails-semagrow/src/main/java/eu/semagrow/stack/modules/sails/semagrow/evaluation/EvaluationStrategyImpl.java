package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.sails.semagrow.algebra.BindJoin;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import eu.semagrow.stack.modules.sails.semagrow.algebra.Transform;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.BindJoinIteration;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.TransformIteration;
import info.aduna.iteration.*;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.federation.JoinExecutorBase;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;
import org.openrdf.query.impl.EmptyBindingSet;

import java.util.ArrayList;

/**
 * Overrides the behavior of the default evaluation strategy implementation.
 * Functionality will be added for (potential) custom operators of the execution plan.
 * @author acharal@iit.demokritos.gr
 */
public class EvaluationStrategyImpl extends org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl
    implements EvaluationStrategy {

    private int batchSize = 10;
    private QueryExecutor queryExecutor;

    public EvaluationStrategyImpl(QueryExecutor queryExecutor, final ValueFactory vf) {
        super(new TripleSource() {
            public CloseableIteration<? extends Statement, QueryEvaluationException>
            getStatements(Resource resource, URI uri, Value value, Resource... resources) throws QueryEvaluationException {
                throw new UnsupportedOperationException("Statement retrieval is not supported");
            }

            public ValueFactory getValueFactory() {
                return vf;
            }
        });
        this.queryExecutor = queryExecutor;
    }

    public EvaluationStrategyImpl(QueryExecutor queryExecutor)
    {

        this(queryExecutor,ValueFactoryImpl.getInstance());
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(UnaryTupleOperator expr, BindingSet bindings) throws QueryEvaluationException {
        if (expr instanceof SourceQuery) {
            return this.evaluate((SourceQuery) expr, bindings);
        } else if (expr instanceof Transform) {
            return this.evaluate((Transform) expr, bindings);
        } else {
            return super.evaluate(expr, bindings);
        }
    }

    public CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(SourceQuery expr, BindingSet bindings) throws QueryEvaluationException {

        return new EmptyIteration<BindingSet, QueryEvaluationException>();
    }

    public CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(Transform expr, BindingSet bindings) throws QueryEvaluationException {

        return new TransformIteration(this.evaluate(expr.getArg(), bindings));
    }

    public CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(Join join, BindingSet bindings) throws QueryEvaluationException {

        if (join instanceof BindJoin)
            return evaluate((BindJoin)join, bindings);
        else
            return super.evaluate(join, bindings);
    }

    public CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(BindJoin join, BindingSet bindings) throws QueryEvaluationException {

        CloseableIteration<BindingSet,QueryEvaluationException> leftIter =
                evaluate(join.getLeftArg(), bindings);
        return new BindJoinIteration(leftIter, join.getRightArg(), bindings, this);
    }

    public CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException
    {
        return new BatchingIteration(bIter, expr, batchSize);
    }

    protected CloseableIteration<BindingSet,QueryEvaluationException>
        evaluateInternal(TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException
    {
        if (expr instanceof SourceQuery)
            return evaluateInternal((SourceQuery)expr, bIter);
        else
            return evaluateInternalDefault(expr, bIter);
    }

    protected CloseableIteration<BindingSet,QueryEvaluationException>
        evaluateInternal(SourceQuery expr, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException {

        // try evaluate with VALUES... else with UNION else default.
        return queryExecutor.evaluate(expr.getSources().get(0), expr.getArg(), bIter);
        // return new EmptyIteration<BindingSet, QueryEvaluationException>();
    }

    protected CloseableIteration<BindingSet,QueryEvaluationException>
        evaluateInternalDefault(TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException {

        return new BatchingIteration(bIter, expr, 1);
    }

    protected class BatchingIteration extends JoinExecutorBase<BindingSet> {

        private final int blockSize;
        private TupleExpr expr;

        public BatchingIteration(CloseableIteration<BindingSet,QueryEvaluationException> leftIter,
                                 TupleExpr expr, int blockSize)
                throws QueryEvaluationException {
            super(leftIter, expr, EmptyBindingSet.getInstance());

            this.expr = expr;
            this.blockSize = blockSize;
            run();
        }

        @Override
        protected void handleBindings() throws Exception {
            while (!closed && leftIter.hasNext()) {

                if (blockSize == 1) {
                    addResult(evaluate(expr, leftIter.next()));
                    continue;
                } else {
                    CloseableIteration<BindingSet, QueryEvaluationException>
                            materializedIter = createBatchIter(leftIter, blockSize);
                    addResult(evaluateInternal(expr,materializedIter));
                }
            }
        }

        protected CloseableIteration<BindingSet, QueryEvaluationException>
            createBatchIter(CloseableIteration<BindingSet, QueryEvaluationException> iter, int blockSize)
                throws QueryEvaluationException {


            ArrayList<BindingSet> blockBindings = new ArrayList<BindingSet>(blockSize);
            for (int i = 0; i < blockSize; i++) {
                if (!iter.hasNext())
                    break;
                blockBindings.add(iter.next());
            }

            CloseableIteration<BindingSet, QueryEvaluationException> materializedIter =
                    new CollectionIteration<BindingSet, QueryEvaluationException>(blockBindings);

            return materializedIter;
        }

    }
}
