package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.api.evaluation.EvaluationStrategy;
import eu.semagrow.stack.modules.api.evaluation.QueryEvaluation;
import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;
import eu.semagrow.stack.modules.api.evaluation.QueryExecutor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.RateIteration;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.QueryEval;

/**
 * Created by angel on 6/11/14.
 */
public class QueryEvaluationImpl implements QueryEvaluation {

    protected final Logger logger = LoggerFactory.getLogger(QueryEvaluationImpl.class);

    public QueryEvaluationSession createSession(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        return new QueryEvaluationSessionImpl();
    }

    protected class QueryEvaluationSessionImpl extends QueryEvaluationSessionImplBase {

        protected EvaluationStrategy getEvaluationStrategyInternal() {
            QueryExecutor queryExecutor = getQueryExecutor();
            EvaluationStrategy evaluationStrategy = new EvaluationStrategyImpl(queryExecutor);
            evaluationStrategy = new MonitoringEvaluationStrategy(evaluationStrategy);
            return evaluationStrategy;
        }

        protected QueryExecutor getQueryExecutor() { return new QueryExecutorImpl(); }

        protected class MonitoringEvaluationStrategy extends EvaluationStrategyWrapper {

            public MonitoringEvaluationStrategy(EvaluationStrategy wrapped) {
                super(wrapped);
            }

            @Override
            public CloseableIteration<BindingSet,QueryEvaluationException>
                evaluate(TupleExpr expr, BindingSet bindings)
                    throws QueryEvaluationException {

                CloseableIteration<BindingSet,QueryEvaluationException> result = super.evaluate(expr,bindings);
                return new RateIterationImpl(result);
            }
        }

        protected class RateIterationImpl extends RateIteration<BindingSet,QueryEvaluationException> {

            public RateIterationImpl(Iteration<BindingSet, QueryEvaluationException> iter) {
                super(iter);
            }

            @Override
            public void handleClose() throws QueryEvaluationException {
                super.handleClose();
                logger.info("Total rows: {}", this.getCount());
                logger.info("Total execution time: {}", this.getRunningTime());
                logger.info("Average consumption rate: {}", this.getAverageConsumedRate());
                logger.info("Average production  rate: {}", this.getAverageProducedRate());
            }
        }
    }
}
