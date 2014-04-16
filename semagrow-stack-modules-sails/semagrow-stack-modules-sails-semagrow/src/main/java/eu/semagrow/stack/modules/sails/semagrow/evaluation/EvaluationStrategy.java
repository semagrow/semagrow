package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;

/**
 * Overrides the behavior of the default evaluation strategy implementation.
 * Functionality will be added for (potential) custom operators of the execution plan.
 * @author acharal@iit.demokritos.gr
 */
public class EvaluationStrategy extends EvaluationStrategyImpl {

    /*
    public EvaluationStrategy(FederatedServiceResolver serviceResolver)
    {
        super(new TripleSource() {
            public CloseableIteration<? extends Statement, QueryEvaluationException>
                    getStatements(Resource resource, URI uri, Value value, Resource... resources) throws QueryEvaluationException {
                return null;
            }

            public ValueFactory getValueFactory() {
                return ValueFactoryImpl.getInstance();
            }
        }, serviceResolver);

    }
    */

    public EvaluationStrategy()
    {
        super(new TripleSource() {
            public CloseableIteration<? extends Statement, QueryEvaluationException>
            getStatements(Resource resource, URI uri, Value value, Resource... resources) throws QueryEvaluationException {
                return null;
            }

            public ValueFactory getValueFactory() {
                return ValueFactoryImpl.getInstance();
            }
        });
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException>
            evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {

        return super.evaluate(expr, bindings);
    }

}
