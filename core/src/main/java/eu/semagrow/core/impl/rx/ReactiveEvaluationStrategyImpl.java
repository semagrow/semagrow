package eu.semagrow.core.impl.rx;

import info.aduna.iteration.Iteration;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;
import org.openrdf.query.algebra.evaluation.util.OrderComparator;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;
import org.openrdf.util.iterators.Iterators;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;

import java.util.*;

/**
 * Created by angel on 11/22/14.
 */
public class ReactiveEvaluationStrategyImpl
        extends EvaluationStrategyImpl
        implements ReactiveEvaluationStrategy
{


    public ReactiveEvaluationStrategyImpl(TripleSource tripleSource, Dataset dataset) {
        super(tripleSource, dataset);
    }

    public ReactiveEvaluationStrategyImpl(TripleSource tripleSource) {
        super(tripleSource);
    }

    public Publisher<BindingSet> evaluateReactive(TupleExpr expr, BindingSet bindings)
        throws QueryEvaluationException
    {
        return RxReactiveStreams.toPublisher(evaluateReactiveInternal(expr, bindings));
    }

    public Observable<BindingSet> evaluateReactiveInternal(TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof StatementPattern) {
            return evaluateReactiveInternal((StatementPattern) expr, bindings);
        }
        else if (expr instanceof UnaryTupleOperator) {
            return evaluateReactiveInternal((UnaryTupleOperator) expr, bindings);
        }
        else if (expr instanceof BinaryTupleOperator) {
            return evaluateReactiveInternal((BinaryTupleOperator) expr, bindings);
        }
        else if (expr instanceof SingletonSet) {
            return evaluateReactiveInternal((SingletonSet) expr, bindings);
        }
        else if (expr instanceof EmptySet) {
            return evaluateReactiveInternal((EmptySet) expr, bindings);
        }
        else if (expr instanceof ExternalSet) {
            return evaluateReactiveInternal((ExternalSet) expr, bindings);
        }
        else if (expr instanceof ZeroLengthPath) {
            return evaluateReactiveInternal((ZeroLengthPath) expr, bindings);
        }
        else if (expr instanceof ArbitraryLengthPath) {
            return evaluateReactiveInternal((ArbitraryLengthPath) expr, bindings);
        }
        else if (expr instanceof BindingSetAssignment) {
            return evaluateReactiveInternal((BindingSetAssignment) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }


    public Observable<BindingSet> evaluateReactiveInternal(UnaryTupleOperator expr, BindingSet bindings)
            throws QueryEvaluationException
    {

        if (expr instanceof Projection) {
            return evaluateReactiveInternal((Projection) expr, bindings);
        }
        else if (expr instanceof MultiProjection) {
            return evaluateReactiveInternal((MultiProjection) expr, bindings);
        }
        else if (expr instanceof Filter) {
            return evaluateReactiveInternal((Filter) expr, bindings);
        }
        else if (expr instanceof Extension) {
            return evaluateReactiveInternal((Extension) expr, bindings);
        }
        else if (expr instanceof Group) {
            return evaluateReactiveInternal((Group) expr, bindings);
        }
        else if (expr instanceof Order) {
            return evaluateReactiveInternal((Order) expr, bindings);
        }
        else if (expr instanceof Slice) {
            return evaluateReactiveInternal((Slice) expr, bindings);
        }
        else if (expr instanceof Distinct) {
            return evaluateReactiveInternal((Distinct) expr, bindings);
        }
        else if (expr instanceof Reduced) {
            return evaluateReactiveInternal((Reduced) expr, bindings);
        }
        else if (expr instanceof Service) {
            return evaluateReactiveInternal((Service) expr, bindings);
        }
        else if (expr instanceof QueryRoot) {
            return evaluateReactiveInternal(expr.getArg(), bindings);
        }
        else if (expr instanceof DescribeOperator) {
            return evaluateReactiveInternal((DescribeOperator) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    public Observable<BindingSet> evaluateReactiveInternal(BinaryTupleOperator expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof Union) {
            return evaluateReactiveInternal((Union) expr, bindings);
        }
        else if (expr instanceof Join) {
            return evaluateReactiveInternal((Join) expr, bindings);
        }
        else if (expr instanceof LeftJoin) {
            return evaluateReactiveInternal((LeftJoin) expr, bindings);
        }
        else if (expr instanceof Intersection) {
            return evaluateReactiveInternal((Intersection) expr, bindings);
        }
        else if (expr instanceof Difference) {
            return evaluateReactiveInternal((Difference) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    public Observable<BindingSet> evaluateReactiveInternal(SingletonSet expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return Observable.just(bindings);
    }

    public Observable<BindingSet> evaluateReactiveInternal(EmptySet expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return Observable.empty();
    }

    public Observable<BindingSet> evaluateReactiveInternal(StatementPattern expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(evaluate(expr, bindings));
    }

    public Observable<BindingSet> evaluateReactiveInternal(BindingSetAssignment expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        final Iterator<BindingSet> iter = expr.getBindingSets().iterator();

        final List<BindingSet> blist = new LinkedList();
        Iterators.addAll(iter, blist);

        return Observable.from(blist)
                .map((b) -> {
                    QueryBindingSet bb = new QueryBindingSet(bindings);
                    bb.addAll(b);
                    return bb;
                });
    }

    public Observable<BindingSet> evaluateReactiveInternal(ExternalSet expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(expr.evaluate(bindings));
    }


    public Observable<BindingSet> evaluateReactiveInternal(ZeroLengthPath expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Observable<BindingSet> evaluateReactiveInternal(ArbitraryLengthPath expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }

    public Observable<BindingSet> evaluateReactiveInternal(Filter expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        QueryBindingSet scopeBindings = new QueryBindingSet(bindings);

        return evaluateReactiveInternal(expr.getArg(), bindings)
                    .filter((b) ->  {
                        try {
                            return this.isTrue(expr.getCondition(), scopeBindings);
                        }catch(QueryEvaluationException /*| ValueExprEvaluationException */ e) {
                            return false;
                        } });
    }

    public Observable<BindingSet> evaluateReactiveInternal(Projection expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactiveInternal(expr.getArg(), bindings)
                .map((b) -> project(expr.getProjectionElemList(), b, bindings));
    }

    public Observable<BindingSet> evaluateReactiveInternal(Extension expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactiveInternal(expr.getArg(), bindings)
                .concatMap((b) -> {
                    try {
                        return Observable.just(extend(expr.getElements(), b));
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                }).onErrorResumeNext(Observable::error);
    }

    public Observable<BindingSet> evaluateReactiveInternal(Union expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return Observable.merge(
                this.evaluateReactiveInternal(expr.getLeftArg(), bindings),
                this.evaluateReactiveInternal(expr.getRightArg(), bindings));
    }

    public Observable<BindingSet> evaluateReactiveInternal(Join expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactiveInternal(expr.getLeftArg(), bindings)
                    .concatMap( (b) -> {
                        try {
                            return this.evaluateReactiveInternal(expr.getRightArg(), b);
                        } catch (Exception e) { return Observable.error(e); } });
    }

    public Observable<BindingSet> evaluateReactiveInternal(LeftJoin expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Observable<BindingSet> r = evaluateReactiveInternal(expr.getRightArg(), bindings);

        Set<String> joinAttributes = expr.getLeftArg().getBindingNames();
        joinAttributes.retainAll(expr.getRightArg().getBindingNames());

        return evaluateReactiveInternal(expr.getLeftArg(), bindings)
                .concatMap( (b) -> {
                    try {
                        return this.evaluateReactiveInternal(expr.getRightArg(), b).defaultIfEmpty(b);
                    } catch (Exception e) {
                        return Observable.error(e); }
                });
    }

    public Observable<BindingSet> evaluateReactiveInternal(Group expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return null;
    }

    public Observable<BindingSet> evaluateReactiveInternal(Order expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        ValueComparator vcmp = new ValueComparator();
        OrderComparator cmp = new OrderComparator(this, expr, vcmp);
        return evaluateReactiveInternal(expr.getArg(), bindings)
                .toSortedList(cmp::compare)
                .flatMap(Observable::from);
    }

    public Observable<BindingSet> evaluateReactiveInternal(Slice expr, BindingSet bindings)
            throws QueryEvaluationException {
        Observable<BindingSet> result = evaluateReactiveInternal(expr.getArg(), bindings);

        if (expr.hasOffset())
            result = result.skip((int) expr.getOffset());

        if (expr.hasLimit())
            result = result.take((int) expr.getLimit());

        return result;
    }

    public Observable<BindingSet> evaluateReactiveInternal(Distinct expr, BindingSet bindings)
            throws QueryEvaluationException {

        return evaluateReactiveInternal(expr.getArg(), bindings).distinct();
    }

    public Observable<BindingSet> evaluateReactiveInternal(Reduced expr, BindingSet bindings)
            throws QueryEvaluationException {

        return evaluateReactiveInternal(expr.getArg(), bindings).distinctUntilChanged();
    }

    public Observable<BindingSet> evaluateReactiveInternal(DescribeOperator expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }

    public Observable<BindingSet> evaluateReactiveInternal(Intersection expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Observable<BindingSet> evaluateReactiveInternal(Difference expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Observable<BindingSet> evaluateReactiveInternal(Service expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }

    protected <T> Observable<T> fromIteration(Iteration<T, ? extends Exception> it) {
        //return Observable.create(new OnSubscribeFromIteration<T>(it));
        return RxReactiveStreams.toObservable(new PublisherFromIteration(it));
    }


    public static BindingSet project(ProjectionElemList projElemList, BindingSet sourceBindings,
                                     BindingSet parentBindings)
    {
        QueryBindingSet resultBindings = new QueryBindingSet(parentBindings);

        for (ProjectionElem pe : projElemList.getElements()) {
            Value targetValue = sourceBindings.getValue(pe.getSourceName());
            if (targetValue != null) {
                // Potentially overwrites bindings from super
                resultBindings.setBinding(pe.getTargetName(), targetValue);
            }
        }

        return resultBindings;
    }

    public BindingSet extend(Collection<ExtensionElem> extElems, BindingSet sourceBindings)
            throws QueryEvaluationException
    {
        QueryBindingSet targetBindings = new QueryBindingSet(sourceBindings);

        for (ExtensionElem extElem : extElems) {
            ValueExpr expr = extElem.getExpr();
            if (!(expr instanceof AggregateOperator)) {
                try {
                    // we evaluate each extension element over the targetbindings, so that bindings from
                    // a previous extension element in this same extension can be used by other extension elements.
                    // e.g. if a projection contains (?a + ?b as ?c) (?c * 2 as ?d)
                    Value targetValue = evaluate(extElem.getExpr(), targetBindings);

                    if (targetValue != null) {
                        // Potentially overwrites bindings from super
                        targetBindings.setBinding(extElem.getName(), targetValue);
                    }
                } catch (ValueExprEvaluationException e) {
                    // silently ignore type errors in extension arguments. They should not cause the
                    // query to fail but just result in no additional binding.
                }
            }
        }

        return targetBindings;
    }

    public static BindingSet joinBindings(BindingSet b1, BindingSet b2) {
        QueryBindingSet result = new QueryBindingSet();

        for (Binding b : b1) {
            if (!result.hasBinding(b.getName()))
                result.addBinding(b);
        }

        for (String name : b2.getBindingNames()) {
            Binding b = b2.getBinding(name);
            if (!result.hasBinding(name)) {
                result.addBinding(b);
            }
        }
        return result;
    }

    public static BindingSet calcKey(BindingSet bindings, Set<String> commonVars) {
        QueryBindingSet q = new QueryBindingSet();
        for (String varName : commonVars) {
            Binding b = bindings.getBinding(varName);
            if (b != null) {
                q.addBinding(b);
            }
        }
        return q;
    }
}
