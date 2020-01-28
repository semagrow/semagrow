package org.semagrow.evaluation.reactor;

import org.semagrow.algebra.TupleExprs;
import org.semagrow.evaluation.QueryExecutorResolver;
import org.semagrow.evaluation.SimpleQueryExecutorResolver;
import org.semagrow.evaluation.util.BindingSetUtil;
import org.semagrow.evaluation.util.LoggingUtil;
import org.semagrow.plan.Plan;
import org.semagrow.evaluation.QueryExecutor;

import org.semagrow.plan.operators.*;
import org.semagrow.selector.Site;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Set;

/**
 * Created by antonis on 26/3/2015.
 */
public class FederatedEvaluationStrategyImpl extends EvaluationStrategyImpl {

    private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);

    public QueryExecutorResolver queryExecutorResolver;

    private int batchSize = 1;

    public FederatedEvaluationStrategyImpl(final ValueFactory vf) {
        super(new TripleSource() {
            public CloseableIteration<? extends Statement, QueryEvaluationException>
            getStatements(Resource resource, IRI uri, Value value, Resource... resources) throws QueryEvaluationException {
                throw new UnsupportedOperationException("Statement retrieval is not supported");
            }

            public ValueFactory getValueFactory() {
                return vf;
            }
        });

        this.queryExecutorResolver = new SimpleQueryExecutorResolver();

    }


    public void setBatchSize(int b) {

    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public Flux<BindingSet> evaluateReactorInternal(TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof SourceQuery) {
            return evaluateReactorInternal((SourceQuery) expr, bindings);
        }
        else if (expr instanceof Plan) {
            return evaluateReactorInternal(((Plan) expr).getArg(), bindings);
        }
        else
            return super.evaluateReactorInternal(expr, bindings);
    }


    @Override
    public Flux<BindingSet> evaluateReactorInternal(Join expr, BindingSet bindings)
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

    public Flux<BindingSet> evaluateReactorInternal(BindJoin expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return this.evaluateReactorInternal(expr.getLeftArg(), bindings)
                .buffer(getBatchSize())
                .flatMap((b) -> {
                    try {
                        return evaluateReactorInternal(expr.getRightArg(), b);
                    } catch (Exception e) {
                        return Flux.error(e);
                    }
                });
    }

    public Flux<BindingSet> evaluateReactorInternal(SourceQuery expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        logger.info("sq {} - Source query [{}] at source {}",
                Math.abs(expr.hashCode()),
                expr.getArg(),
                expr.getSite());

        return evaluateSourceReactive(expr.getSite(), expr.getArg(), bindings);
    }


    public Flux<BindingSet> evaluateSourceReactive(Site source, TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Set<String> free = TupleExprs.getFreeVariables(expr);
        BindingSet relevant = bindingSetOps.project(free, bindings);
        QueryExecutor executor = queryExecutorResolver.resolve(source)
                .orElseThrow( () -> new QueryEvaluationException("Cannot find executor for source " + source));

        if (BindingSetUtil.hasBNode(relevant))
            return Flux.empty();
        else {
            //Publisher<BindingSet> result = queryExecutor.evaluate(source, expr, bindings);
            Publisher<BindingSet> result = executor.evaluate(source, expr, bindings);
            return Flux.from(result);
        }
    }

    public Flux<BindingSet> evaluateSourceReactive(Site source, TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException
    {
        Set<String> free = TupleExprs.getFreeVariables(expr);
        QueryExecutor executor = queryExecutorResolver.resolve(source)
                .orElseThrow( () -> new QueryEvaluationException("Cannot find executor for source " + source));

        return Flux.fromIterable(bindings)
                .filter((b) -> !BindingSetUtil.hasBNode(bindingSetOps.project(free, b)))
                .buffer(getBatchSize())
                .flatMap((bl) -> {
                    Publisher<BindingSet> result = null;
                    if (bl.isEmpty())
                        return Flux.empty();
                    try {
                        //result = queryExecutor.evaluate(source, expr, bl);
                        result = executor.evaluate(source, expr, bl);
                        return Flux.from(result);
                    } catch (QueryEvaluationException e) {
                        return Flux.error(e);
                    }

                });
    }

    public Flux<BindingSet> evaluateReactorInternal(TupleExpr expr, List<BindingSet> bindingList)
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

    public Flux<BindingSet> evaluateReactorInternal(SourceQuery expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        LoggingUtil.logSourceQuery(logger, expr);

        //return queryExecutor.evaluateReactorInternal(null, expr.getArg(), bindings)
        return evaluateSourceReactive(expr.getSite(), expr.getArg(), bindingList);

    }

    public Flux<BindingSet> evaluateReactiveDefault(TupleExpr expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        Set<String> freeVars = TupleExprs.getFreeVariables(expr);


        return Flux.fromIterable(bindingList)
                .filter((b) -> !BindingSetUtil.hasBNode(bindingSetOps.project(freeVars,b)))
                .flatMap(b -> {
                    try {
                        return evaluateReactorInternal(expr, b);
                    } catch (Exception e) {
                        return Flux.error(e);
                    }
                });
    }

    public Flux<BindingSet> evaluateReactorInternal(Union expr, List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        return Flux.just(expr.getLeftArg(), expr.getRightArg())
                .flatMap(e -> {
                    try {
                        return evaluateReactorInternal(e, bindingList);
                    } catch (Exception x) {
                        return Flux.error(x);
                }});
    }

}

