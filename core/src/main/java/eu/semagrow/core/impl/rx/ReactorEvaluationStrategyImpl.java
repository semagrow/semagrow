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
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.*;


/**
 * Created by antonis on 26/3/2015.
 */
public class ReactorEvaluationStrategyImpl
        extends EvaluationStrategyImpl
        implements ReactiveEvaluationStrategy {

    public ReactorEvaluationStrategyImpl(TripleSource tripleSource) {
        super(tripleSource);
    }

    public ReactorEvaluationStrategyImpl(TripleSource tripleSource, Dataset dataset) {
        super(tripleSource, dataset);
    }

    @Override
    public Publisher<BindingSet> evaluateReactive(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        //return RxReactiveStreams.toPublisher(evaluateReactorInternal(expr, bindings));;
        return evaluateReactorInternal(expr, bindings);
    }

    public Stream<BindingSet> evaluateReactorInternal(TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof StatementPattern) {
            return evaluateReactorInternal((StatementPattern) expr, bindings);
        }
        else if (expr instanceof UnaryTupleOperator) {
            return evaluateReactorInternal((UnaryTupleOperator) expr, bindings);
        }
        else if (expr instanceof BinaryTupleOperator) {
            return evaluateReactorInternal((BinaryTupleOperator) expr, bindings);
        }
        else if (expr instanceof SingletonSet) {
            return evaluateReactorInternal((SingletonSet) expr, bindings);
        }
        else if (expr instanceof EmptySet) {
            return evaluateReactorInternal((EmptySet) expr, bindings);
        }
        else if (expr instanceof ExternalSet) {
            return evaluateReactorInternal((ExternalSet) expr, bindings);
        }
        else if (expr instanceof ZeroLengthPath) {
            return evaluateReactorInternal((ZeroLengthPath) expr, bindings);
        }
        else if (expr instanceof ArbitraryLengthPath) {
            return evaluateReactorInternal((ArbitraryLengthPath) expr, bindings);
        }
        else if (expr instanceof BindingSetAssignment) {
            return evaluateReactorInternal((BindingSetAssignment) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    public Stream<BindingSet> evaluateReactorInternal(UnaryTupleOperator expr, BindingSet bindings)
            throws QueryEvaluationException
    {

        if (expr instanceof Projection) {
            return evaluateReactorInternal((Projection) expr, bindings);
        }
        else if (expr instanceof MultiProjection) {
            return evaluateReactorInternal((MultiProjection) expr, bindings);
        }
        else if (expr instanceof Filter) {
            return evaluateReactorInternal((Filter) expr, bindings);
        }
        else if (expr instanceof Extension) {
            return evaluateReactorInternal((Extension) expr, bindings);
        }
        else if (expr instanceof Group) {
            return evaluateReactorInternal((Group) expr, bindings);
        }
        else if (expr instanceof Order) {
            return evaluateReactorInternal((Order) expr, bindings);
        }
        else if (expr instanceof Slice) {
            return evaluateReactorInternal((Slice) expr, bindings);
        }
        else if (expr instanceof Distinct) {
            return evaluateReactorInternal((Distinct) expr, bindings);
        }
        else if (expr instanceof Reduced) {
            return evaluateReactorInternal((Reduced) expr, bindings);
        }
        else if (expr instanceof Service) {
            return evaluateReactorInternal((Service) expr, bindings);
        }
        else if (expr instanceof QueryRoot) {
            return evaluateReactorInternal(expr.getArg(), bindings);
        }
        else if (expr instanceof DescribeOperator) {
            return evaluateReactorInternal((DescribeOperator) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    public Stream<BindingSet> evaluateReactorInternal(BinaryTupleOperator expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        if (expr instanceof Union) {
            return evaluateReactorInternal((Union) expr, bindings);
        }
        else if (expr instanceof Join) {
            return evaluateReactorInternal((Join) expr, bindings);
        }
        else if (expr instanceof LeftJoin) {
            return evaluateReactorInternal((LeftJoin) expr, bindings);
        }
        else if (expr instanceof Intersection) {
            return evaluateReactorInternal((Intersection) expr, bindings);
        }
        else if (expr instanceof Difference) {
            return evaluateReactorInternal((Difference) expr, bindings);
        }
        else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        }
        else {
            throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
        }
    }

    public Stream<BindingSet> evaluateReactorInternal(SingletonSet expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return Streams.just(bindings);
    }

    public Stream<BindingSet> evaluateReactorInternal(EmptySet expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return Streams.empty();
    }

    public Stream<BindingSet> evaluateReactorInternal(StatementPattern expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(evaluate(expr, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(BindingSetAssignment expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        final Iterator<BindingSet> iter = expr.getBindingSets().iterator();

        final List<BindingSet> blist = new LinkedList();
        Iterators.addAll(iter, blist);

        return Streams.from(blist)
                .map((b) -> {
                    QueryBindingSet bb = new QueryBindingSet(bindings);
                    bb.addAll(b);
                    return bb;
                });
    }

    public Stream<BindingSet> evaluateReactorInternal(ExternalSet expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(expr.evaluate(bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(ZeroLengthPath expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(ArbitraryLengthPath expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Filter expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        QueryBindingSet scopeBindings = new QueryBindingSet(bindings);

        return evaluateReactorInternal(expr.getArg(), bindings)
                .filter((b) ->  {
                    try {
                        return this.isTrue(expr.getCondition(), scopeBindings);
                    }catch(QueryEvaluationException /*| ValueExprEvaluationException */ e) {
                        return false;
                    } });
    }

    public Stream<BindingSet> evaluateReactorInternal(Projection expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorInternal(expr.getArg(), bindings)
                .map((b) -> project(expr.getProjectionElemList(), b, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Extension expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorInternal(expr.getArg(), bindings)
                .concatMap((b) -> {
                    try {
                        return Streams.just(extend(expr.getElements(), b));
                    } catch (Exception e) {
                        return Streams.fail(e);
                    }
                });
    }

    public Stream<BindingSet> evaluateReactorInternal(Union expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return Streams.merge(
                this.evaluateReactorInternal(expr.getLeftArg(), bindings),
                this.evaluateReactorInternal(expr.getRightArg(), bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Join expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorInternal(expr.getLeftArg(), bindings)
                .concatMap( (b) -> {
                    try {
                        return this.evaluateReactorInternal(expr.getRightArg(), b);
                    } catch (Exception e) { return Streams.fail(e); } });
    }

    public Stream<BindingSet> evaluateReactorInternal(LeftJoin expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Stream<BindingSet> r = evaluateReactorInternal(expr.getRightArg(), bindings);

        Set<String> joinAttributes = expr.getLeftArg().getBindingNames();
        joinAttributes.retainAll(expr.getRightArg().getBindingNames());

        return evaluateReactorInternal(expr.getLeftArg(), bindings)
                .concatMap( (b) -> {
                    try {
                        return this.evaluateReactorInternal(expr.getRightArg(), b).defaultIfEmpty(b);
                    } catch (Exception e) {
                        return Streams.fail(e); }
                });
    }

    public Stream<BindingSet> evaluateReactorInternal(Group expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return null;
    }

    public Stream<BindingSet> evaluateReactorInternal(Order expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        ValueComparator vcmp = new ValueComparator();
        OrderComparator cmp = new OrderComparator(this, expr, vcmp);
        /*return evaluateReactorInternal(expr.getArg(), bindings)
                .toSortedList(cmp::compare)
                .flatMap(Streams::from);*/
        return evaluateReactorInternal(expr.getArg(), bindings)
                .sort(cmp::compare);
    }

    public Stream<BindingSet> evaluateReactorInternal(Slice expr, BindingSet bindings)
            throws QueryEvaluationException {
        Stream<BindingSet> result = evaluateReactorInternal(expr.getArg(), bindings);

        if (expr.hasOffset())
            result = result.skip((int) expr.getOffset());

        if (expr.hasLimit())
            result = result.take((int) expr.getLimit());

        return result;
    }

    public Stream<BindingSet> evaluateReactorInternal(Distinct expr, BindingSet bindings)
            throws QueryEvaluationException {

        return evaluateReactorInternal(expr.getArg(), bindings).distinct();
    }

    public Stream<BindingSet> evaluateReactorInternal(Reduced expr, BindingSet bindings)
            throws QueryEvaluationException {

        return evaluateReactorInternal(expr.getArg(), bindings).distinctUntilChanged();
    }

    public Stream<BindingSet> evaluateReactorInternal(DescribeOperator expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Intersection expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(Difference expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(Service expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(this.evaluate(expr, bindings));
    }

    protected <T> Stream<T> fromIteration(Iteration<T, ? extends Exception> it) {
        return Streams.wrap(new PublisherFromIteration(it));
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
