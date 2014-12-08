package eu.semagrow.stack.modules.sails.semagrow.evaluation.monitoring;

import eu.semagrow.stack.modules.api.evaluation.QueryEvaluationSession;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.AbstractEvaluationSessionAwareInterceptor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.interceptors.QueryEvaluationInterceptor;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by angel on 6/27/14.
 */
public class MeasuringInterceptor extends AbstractEvaluationSessionAwareInterceptor
        implements QueryEvaluationInterceptor {

    private Map<TupleExpr, MeasurementPoint> measurements = new HashMap<TupleExpr, MeasurementPoint>();

    private QueryEvaluationSession session;

    public void setQueryEvaluationSession(QueryEvaluationSession session) { this.session = session; }

    public QueryEvaluationSession getQueryEvaluationSession() { return session; }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        afterEvaluation(TupleExpr expr, BindingSet bindings,
                        CloseableIteration<BindingSet, QueryEvaluationException> result) {

        MeasuringIteration<BindingSet,QueryEvaluationException> measuringResult =
                new MeasuringIteration<BindingSet,QueryEvaluationException>(result);

        saveAsMeasurementPoint(expr, measuringResult);

        return measuringResult;
    }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        afterEvaluation(TupleExpr expr, CloseableIteration<BindingSet, QueryEvaluationException> bindings, CloseableIteration<BindingSet, QueryEvaluationException> result) {

        MeasuringIteration<BindingSet,QueryEvaluationException> measuringResult =
                new MeasuringIteration<BindingSet,QueryEvaluationException>(result);

        saveAsMeasurementPoint(expr, measuringResult);

        return measuringResult;
    }

    public Collection<MeasurementPoint> getMeasurements() { return measurements.values(); }

    private void saveAsMeasurementPoint(final TupleExpr expr,
            final MeasuringIteration<BindingSet,QueryEvaluationException> measuringIteration)
    {
        if (!measurements.containsKey(expr)) {
            MeasurementPoint point = new MeasurementPoint() {

                public String getId() {
                    return expr.toString();
                }

                public long getCount() {
                    return measuringIteration.getCount();
                }

                public long getRunningTime() {
                    return measuringIteration.getRunningTime();
                }
            };

            measurements.put(expr, point);
        }
    }

}
