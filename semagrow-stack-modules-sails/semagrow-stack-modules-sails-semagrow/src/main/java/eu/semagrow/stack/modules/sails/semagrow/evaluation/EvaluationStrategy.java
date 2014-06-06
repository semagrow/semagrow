package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import eu.semagrow.stack.modules.sails.semagrow.algebra.Transform;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.TransformIteration;
import info.aduna.iteration.*;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;

import java.io.Closeable;

/**
 * Overrides the behavior of the default evaluation strategy implementation.
 * Functionality will be added for (potential) custom operators of the execution plan.
 * @author acharal@iit.demokritos.gr
 */
public class EvaluationStrategy extends EvaluationStrategyImpl {

    public EvaluationStrategy(final ValueFactory vf) {
        super(new TripleSource() {
            public CloseableIteration<? extends Statement, QueryEvaluationException>
            getStatements(Resource resource, URI uri, Value value, Resource... resources) throws QueryEvaluationException {
                throw new UnsupportedOperationException("Statement retrieval is not supported");
            }

            public ValueFactory getValueFactory() {
                return vf;
            }
        });
    }

    public EvaluationStrategy()
    {
        this(ValueFactoryImpl.getInstance());
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

        return super.evaluate(join, bindings);

    }

    public CloseableIteration<BindingSet,QueryEvaluationException>
        evaluate(TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bIter)
            throws QueryEvaluationException
    {
        return null;
    }
}
