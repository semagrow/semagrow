package eu.semagrow.core.impl.evaluation.rx.reactor;

import eu.semagrow.core.impl.algebra.*;
import eu.semagrow.core.impl.planner.Plan;
import eu.semagrow.core.impl.evaluation.rx.QueryExecutor;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.List;

/**
 * Created by antonis on 26/3/2015.
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
        if (!Environment.alive())
            Environment.initialize();
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
    public Stream<BindingSet> evaluateReactorInternal(TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof SourceQuery) {
            return evaluateReactorInternal((SourceQuery) expr, bindings);
        }
        else if (expr instanceof Join) {
            return evaluateReactorInternal((Join) expr, bindings);
        }
        else if (expr instanceof Plan) {
            return evaluateReactorInternal(((Plan) expr).getArg(), bindings);
        }
        else if (expr instanceof Transform) {
            return evaluateReactorInternal((Transform) expr, bindings);
        }
        else
            return super.evaluateReactorInternal(expr, bindings);
    }


    @Override
    public Stream<BindingSet> evaluateReactorInternal(Join expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof BindJoin) {
            return evaluateReactorInternal((BindJoin) expr, bindings);
        }
        else if (expr instanceof HashJoin) {
            return evaluateReactorInternal((HashJoin) expr, bindings);
        }
        else if (expr instanceof MergeJoin) {
            return evaluateReactorInternal((MergeJoin) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    /*
    private static <T, K, E> Stream<Map<K, List<E>>> toMultiMap(Stream<T> s, reactor.fn.Function<T, K> k, reactor.fn.Function<T,E> e) {
        s.groupBy(k)
    }
    */
    /*
    public Stream<BindingSet> evaluateReactorInternal(HashJoin expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Stream<BindingSet> r = evaluateReactorInternal(expr.getRightArg(), bindings);

        Set<String> joinAttributes = expr.getLeftArg().getBindingNames();
        joinAttributes.retainAll(expr.getRightArg().getBindingNames());

        return evaluateReactorInternal(expr.getLeftArg(), bindings)
                .toMultimap(b -> calcKey(b, joinAttributes), b1 -> b1)
                .flatMap((probe) ->
                                r.concatMap(b -> {
                                    if (!probe.containsKey(calcKey(b, joinAttributes)))
                                        return Stream.empty();
                                    else
                                        return Stream.from(probe.get(calcKey(b, joinAttributes)))
                                                .merge(Stream.just(b),
                                                        b1 -> Stream.never(),
                                                        b1 -> Stream.never(),
                                                        FederatedReactiveEvaluationStrategyImpl::joinBindings);
                                })
                );
    }

    public static Stream<BindingSet> hashJoin(Stream<BindingSet> left, Stream<BindingSet> right, Set<String> joinAttributes) {
        return left.toMultimap(b -> calcKey(b, joinAttributes), b1 -> b1)
                .flatMap((probe) -> right.concatMap(b -> {
                            if (!probe.containsKey(calcKey(b, joinAttributes)))
                                return Streams.empty();
                            else
                                return Streams.from(probe.get(calcKey(b, joinAttributes)))
                                        .merge(Streams.from(b),
                                                b1 -> Stream.never(),
                                                b1 -> Stream.never(),
                                                FederatedReactiveEvaluationStrategyImpl::joinBindings);
                        })
                );
    }
    */

    public Stream<BindingSet> evaluateReactorInternal(BindJoin expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return this.evaluateReactorInternal(expr.getLeftArg(), bindings)
                .buffer(getBatchSize())
                .flatMap((b) -> {
                    try {
                        return evaluateReactorInternal(expr.getRightArg(), b);
                    } catch (Exception e) {
                        return Streams.fail(e);
                    }
                });
                //.dispatchOn(Environment.cachedDispatcher());
    }

    public Stream<BindingSet> evaluateReactorInternal(SourceQuery expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        //return queryExecutor.evaluateReactorInternal(null, expr.getArg(), bindings)
        if (expr.getSources().size() == 0)
            return Streams.empty();
        else if (expr.getSources().size() == 1)
            return evaluateSourceReactive(expr.getSources().get(0), expr.getArg(), bindings);
        else {
            return Streams.from(expr.getSources())
                    .flatMap((s) -> {
                        try {
                            return evaluateSourceReactive(s, expr.getArg(), bindings);
                        } catch (QueryEvaluationException e) { return Streams.fail(e); } });
        }
    }


    public Stream<BindingSet> evaluateSourceReactive(URI source, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Publisher<BindingSet> result = queryExecutor.evaluate(source, expr, bindings);
        return Streams.wrap(result).subscribeOn(Environment.dispatcher(Environment.THREAD_POOL));
    }

    public Stream<BindingSet> evaluateSourceReactive(URI source, TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException
    {
        Publisher<BindingSet> result = queryExecutor.evaluate(source, expr, bindings);
        return Streams.wrap(result).subscribeOn(Environment.dispatcher(Environment.THREAD_POOL));
    }

    public Stream<BindingSet> evaluateReactorInternal(Transform expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return this.evaluateReactorInternal(expr.getArg(), bindings);
    }


    public Stream<BindingSet> evaluateReactorInternal(TupleExpr expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        if (expr instanceof Plan)
            return evaluateReactorInternal(((Plan) expr).getArg(), bindingList);
        else if (expr instanceof Union)
            return evaluateReactorInternal((Union) expr, bindingList);
        else if (expr instanceof SourceQuery)
            return evaluateReactorInternal((SourceQuery) expr, bindingList);
        else
            return evaluateReactiveDefault(expr, bindingList);
    }

    public Stream<BindingSet> evaluateReactorInternal(SourceQuery expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        //return queryExecutor.evaluateReactorInternal(null, expr.getArg(), bindings)
        if (expr.getSources().size() == 0)
            return Streams.empty();
        else if (expr.getSources().size() == 1)
            return evaluateSourceReactive(expr.getSources().get(0), expr.getArg(), bindingList);
        else {
            return Streams.from(expr.getSources())
                    .flatMap((s) -> {
                        try {
                            return evaluateSourceReactive(s, expr.getArg(), bindingList);
                        } catch (QueryEvaluationException e) { return Streams.fail(e); } });
        }
    }

    public Stream<BindingSet> evaluateReactiveDefault(TupleExpr expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        return Streams.from(bindingList).flatMap(b -> {
            try {
                return evaluateReactorInternal(expr, b);
            }
            catch (Exception e) {
                return Streams.fail(e);
            }
        });
    }

    public Stream<BindingSet> evaluateReactorInternal(Union expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        return Streams.just(expr.getLeftArg(), expr.getRightArg())
                .flatMap(e -> {
                    try {
                        return evaluateReactorInternal(e, bindingList);
                    } catch (Exception x) {
                        return Streams.fail(x);
                }});
    }
}

