package eu.semagrow.core.impl.evaluation.rx.rxjava;

import eu.semagrow.core.impl.algebra.*;
import eu.semagrow.core.impl.evaluation.util.BindingSetUtil;
import eu.semagrow.core.impl.planner.Plan;
import eu.semagrow.core.impl.evaluation.rx.QueryExecutor;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.Set;

/**
 * Created by angel on 11/26/14.
 */
public class FederatedEvaluationStrategyImpl extends EvaluationStrategyImpl {

    public QueryExecutor queryExecutor;

    private int batchSize = 10;

    public FederatedEvaluationStrategyImpl(QueryExecutor queryExecutor, final ValueFactory vf) {
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

    public FederatedEvaluationStrategyImpl(QueryExecutor queryExecutor) {
        this(queryExecutor, ValueFactoryImpl.getInstance());
    }


    public void setBatchSize(int b) {
        batchSize = b;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public Observable<BindingSet> evaluateReactiveInternal(TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof SourceQuery) {
            return evaluateReactiveInternal((SourceQuery) expr, bindings);
        }
        else if (expr instanceof Join) {
            return evaluateReactiveInternal((Join) expr, bindings);
        }
        else if (expr instanceof Plan) {
            return evaluateReactiveInternal(((Plan) expr).getArg(), bindings);
        }
        else if (expr instanceof Transform) {
            return evaluateReactiveInternal((Transform) expr, bindings);
        }
        else
            return super.evaluateReactiveInternal(expr, bindings);
    }


    @Override
    public Observable<BindingSet> evaluateReactiveInternal(Join expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof BindJoin) {
            return evaluateReactiveInternal((BindJoin) expr, bindings);
        }
        else if (expr instanceof HashJoin) {
            return evaluateReactiveInternal((HashJoin) expr, bindings);
        }
        else if (expr instanceof MergeJoin) {
            return evaluateReactiveInternal((MergeJoin) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    public Observable<BindingSet> evaluateReactiveInternal(HashJoin expr, BindingSet bindings)
        throws QueryEvaluationException
    {
        Observable<BindingSet> r = evaluateReactiveInternal(expr.getRightArg(), bindings);

        Set<String> joinAttributes = expr.getLeftArg().getBindingNames();
        joinAttributes.retainAll(expr.getRightArg().getBindingNames());

        return evaluateReactiveInternal(expr.getLeftArg(), bindings)
                .toMultimap(b -> BindingSetUtil.project(joinAttributes, b), b1 -> b1)
                .flatMap((probe) ->
                    r.concatMap(b -> {
                        if (!probe.containsKey(BindingSetUtil.project(joinAttributes, b)))
                            return Observable.empty();
                        else
                            return Observable.from(probe.get(BindingSetUtil.project(joinAttributes, b)))
                                             .join(Observable.just(b),
                                                     b1 -> Observable.never(),
                                                     b1 -> Observable.never(),
                                                     BindingSetUtil::merge);
                    })
                );
    }

    public static Observable<BindingSet> hashJoin(Observable<BindingSet> left, Observable<BindingSet> right, Set<String> joinAttributes) {
        return left.toMultimap(b -> BindingSetUtil.project(joinAttributes, b), b1 -> b1)
                .flatMap((probe) -> right.concatMap(b -> {
                            if (!probe.containsKey(BindingSetUtil.project(joinAttributes, b)))
                                return Observable.empty();
                            else
                                return Observable.from(probe.get(BindingSetUtil.project(joinAttributes, b)))
                                        .join(Observable.just(b),
                                                b1 -> Observable.never(),
                                                b1 -> Observable.never(),
                                                BindingSetUtil::merge);
                        })
                );
    }


    public Observable<BindingSet> evaluateReactiveInternal(BindJoin expr, BindingSet bindings)
        throws QueryEvaluationException
    {
        return this.evaluateReactiveInternal(expr.getLeftArg(), bindings)
                .filter((b) -> !BindingSetUtil.hasBNode(b))
                .buffer(getBatchSize())
                .flatMap((b) -> {
                    try {
                        return evaluateReactiveInternal(expr.getRightArg(), b);
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                });
    }

    public Observable<BindingSet> evaluateReactiveInternal(SourceQuery expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        //return queryExecutor.evaluateReactiveInternal(null, expr.getArg(), bindings)
        if (expr.getSources().size() == 0)
            return Observable.empty();
        else if (expr.getSources().size() == 1)
            return evaluateSourceReactive(expr.getSources().get(0), expr.getArg(), bindings);
        else {
            return Observable.from(expr.getSources())
                    .flatMap((s) -> {
                            try {
                                return evaluateSourceReactive(s, expr.getArg(), bindings);
                            } catch (QueryEvaluationException e) { return Observable.error(e); } });
        }
    }


    public Observable<BindingSet> evaluateSourceReactive(URI source, TupleExpr expr, BindingSet bindings)
        throws QueryEvaluationException
    {
        Publisher<BindingSet> result = queryExecutor.evaluate(source, expr, bindings);

        return RxReactiveStreams.toObservable(result).subscribeOn(Schedulers.io());
    }

    public Observable<BindingSet> evaluateSourceReactive(URI source, TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException
    {

        Publisher<BindingSet> result = queryExecutor.evaluate(source, expr, bindings);

        return RxReactiveStreams.toObservable(result).subscribeOn(Schedulers.io());
    }

    public Observable<BindingSet> evaluateReactiveInternal(Transform expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return this.evaluateReactiveInternal(expr.getArg(), bindings);
    }


    public Observable<BindingSet> evaluateReactiveInternal(TupleExpr expr, List<BindingSet> bindingList)
        throws QueryEvaluationException
    {
        if (expr instanceof Plan)
            return evaluateReactiveInternal(((Plan) expr).getArg(), bindingList);
        else if (expr instanceof Union)
            return evaluateReactiveInternal((Union) expr, bindingList);
        else if (expr instanceof SourceQuery)
            return evaluateReactiveInternal((SourceQuery) expr, bindingList);
        else
            return evaluateReactiveDefault(expr, bindingList);
    }

    public Observable<BindingSet> evaluateReactiveInternal(SourceQuery expr, List<BindingSet> bindingList)
        throws QueryEvaluationException
    {
        //return queryExecutor.evaluateReactiveInternal(null, expr.getArg(), bindings)
        if (expr.getSources().size() == 0)
            return Observable.empty();
        else if (expr.getSources().size() == 1)
            return evaluateSourceReactive(expr.getSources().get(0), expr.getArg(), bindingList);
        else {
            return Observable.from(expr.getSources())
                    .flatMap((s) -> {
                        try {
                            return evaluateSourceReactive(s, expr.getArg(), bindingList);
                        } catch (QueryEvaluationException e) { return Observable.error(e); } });
        }
    }

    public Observable<BindingSet> evaluateReactiveDefault(TupleExpr expr, List<BindingSet> bindingList)
        throws QueryEvaluationException
    {
        return Observable.from(bindingList).flatMap(b -> {
            try {
                return evaluateReactiveInternal(expr, b);
            }
            catch (Exception e) {
                return Observable.error(e);
            }
        });
    }

    public Observable<BindingSet> evaluateReactiveInternal(Union expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        return Observable.just(expr.getLeftArg(), expr.getRightArg())
                .flatMap(e -> { try {
                    return evaluateReactiveInternal(e, bindingList);
                } catch (Exception x) {
                    return Observable.error(x);
                }});
    }

}
