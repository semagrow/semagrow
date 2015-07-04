package eu.semagrow.core.impl.rx;

import info.aduna.iteration.Iteration;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.vocabulary.XMLSchema;
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
import org.openrdf.query.algebra.evaluation.iterator.GroupIterator;
import org.openrdf.query.algebra.evaluation.util.MathUtil;
import org.openrdf.query.algebra.evaluation.util.OrderComparator;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;
import org.openrdf.util.iterators.Iterators;
import org.reactivestreams.Publisher;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.stream.GroupedStream;

import java.util.*;


/**
 * Created by antonis on 26/3/2015.
 */
public class ReactorEvaluationStrategyImpl
        extends EvaluationStrategyImpl
        implements ReactiveEvaluationStrategy {

    private ValueFactory vf;

    public ReactorEvaluationStrategyImpl(TripleSource tripleSource) {
        super(tripleSource);
        vf = tripleSource.getValueFactory();
    }

    public ReactorEvaluationStrategyImpl(TripleSource tripleSource, Dataset dataset) {
        super(tripleSource, dataset);
        vf = tripleSource.getValueFactory();
    }

    public Value evaluateValue(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return evaluate(expr, bindings);
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
                .filter((b) -> {
                    try {
                        return this.isTrue(expr.getCondition(), scopeBindings);
                    } catch (QueryEvaluationException /*| ValueExprEvaluationException */ e) {
                        return false;
                    }
                });
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
                .concatMap((b) -> {
                    try {
                        return this.evaluateReactorInternal(expr.getRightArg(), b);
                    } catch (Exception e) {
                        return Streams.fail(e);
                    }
                });
    }

    public Stream<BindingSet> evaluateReactorInternal(LeftJoin expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Stream<BindingSet> r = evaluateReactorInternal(expr.getRightArg(), bindings);

        Set<String> joinAttributes = expr.getLeftArg().getBindingNames();
        joinAttributes.retainAll(expr.getRightArg().getBindingNames());

        return evaluateReactorInternal(expr.getLeftArg(), bindings)
                .concatMap((b) -> {
                    try {
                        return this.evaluateReactorInternal(expr.getRightArg(), b).defaultIfEmpty(b);
                    } catch (Exception e) {
                        return Streams.fail(e);
                    }
                });
    }

    public Stream<BindingSet> evaluateReactorInternal(Group expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Set<String> groupByBindings = expr.getGroupBindingNames();

        Stream<BindingSet> s = evaluateReactorInternal(expr.getArg(), bindings);

        Stream<GroupedStream<BindingSet, BindingSet>> g = s.groupBy((b) -> projectSet(groupByBindings, b, bindings));

        //return g.flatMap((gs) -> Streams.just(gs.key()));
        return g.flatMap((gs) -> aggregate(gs, expr, bindings));
    }

    public Stream<BindingSet> aggregate(GroupedStream<BindingSet, BindingSet> g, Group expr, BindingSet parentBindings) {
        BindingSet k = g.key();

        try {
            Entry e = new Entry(k, parentBindings, expr);
            Stream<Entry> s = g.reduce( e, (e1, b) -> { try { e1.addSolution(b); return e1; } catch(Exception x) { return null; } });
            return s.flatMap( (ee) -> {
                QueryBindingSet b = new QueryBindingSet(k);
                try {
                    ee.bindSolution(b);
                    return Streams.just(b);
                } catch(Exception x) {
                    return Streams.fail(x);
                }
            });

        }catch(QueryEvaluationException e)
        {
            return Streams.fail(e);
        }
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
            throws QueryEvaluationException {
        return fromIteration(this.evaluate(expr, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Intersection expr, BindingSet bindings)
            throws QueryEvaluationException {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(Difference expr, BindingSet bindings)
            throws QueryEvaluationException {
        return fromIteration(this.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(Service expr, BindingSet bindings)
            throws QueryEvaluationException {
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


    public static BindingSet projectSet(Set<String> projElemList,
                                     BindingSet sourceBindings,
                                     BindingSet parentBindings)
    {
        QueryBindingSet resultBindings = new QueryBindingSet(parentBindings);

        for (String pe : projElemList) {
            Value targetValue = sourceBindings.getValue(pe);
            if (targetValue != null) {
                // Potentially overwrites bindings from super
                resultBindings.setBinding(pe, targetValue);
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



    private class ConcatAggregate extends Aggregate {
        private StringBuilder concatenated = new StringBuilder();
        private String separator = " ";

        public ConcatAggregate(GroupConcat groupConcatOp, BindingSet parentBindings)
                throws ValueExprEvaluationException, QueryEvaluationException {
            super(groupConcatOp);
            ValueExpr separatorExpr = groupConcatOp.getSeparator();
            if(separatorExpr != null) {
                Value separatorValue = evaluateValue(separatorExpr, parentBindings);
                this.separator = separatorValue.stringValue();
            }

        }

        public void processAggregate(BindingSet s) throws QueryEvaluationException {
            Value v = this.evaluate(s);
            if(v != null && this.distinctValue(v)) {
                this.concatenated.append(v.stringValue());
                this.concatenated.append(this.separator);
            }

        }

        public Value getValue() {
            if(this.concatenated.length() == 0) {
                return vf.createLiteral("");
            } else {
                int len = this.concatenated.length() - this.separator.length();
                return vf.createLiteral(this.concatenated.substring(0, len));
            }
        }
    }

    private class SampleAggregate extends Aggregate {
        private Value sample = null;
        private Random random = new Random(System.currentTimeMillis());

        public SampleAggregate(Sample operator) {
            super(operator);
        }

        public void processAggregate(BindingSet s) throws QueryEvaluationException {
            if(this.sample == null || this.random.nextFloat() < 0.5F) {
                this.sample = this.evaluate(s);
            }

        }

        public Value getValue() {
            return this.sample;
        }
    }

    private class AvgAggregate extends Aggregate {
        private long count = 0L;
        private Literal sum;
        private ValueExprEvaluationException typeError;

        public AvgAggregate(Avg operator) {
            super(operator);
            this.sum = vf.createLiteral("0", XMLSchema.INTEGER);
            this.typeError = null;
        }

        public void processAggregate(BindingSet s) throws QueryEvaluationException {
            if(this.typeError == null) {
                Value v = this.evaluate(s);
                if(this.distinctValue(v)) {
                    if(v instanceof Literal) {
                        Literal nextLiteral = (Literal)v;
                        if(nextLiteral.getDatatype() != null && XMLDatatypeUtil.isNumericDatatype(nextLiteral.getDatatype())) {
                            this.sum = MathUtil.compute(this.sum, nextLiteral, MathExpr.MathOp.PLUS);
                        } else {
                            this.typeError = new ValueExprEvaluationException("not a number: " + v);
                        }

                        ++this.count;
                    } else if(v != null) {
                        this.typeError = new ValueExprEvaluationException("not a number: " + v);
                    }
                }

            }
        }

        public Value getValue() throws ValueExprEvaluationException {
            if(this.typeError != null) {
                throw this.typeError;
            } else if(this.count == 0L) {
                return vf.createLiteral(0.0D);
            } else {
                Literal sizeLit = vf.createLiteral(this.count);
                return MathUtil.compute(this.sum, sizeLit, MathExpr.MathOp.DIVIDE);
            }
        }
    }

    private class SumAggregate extends Aggregate {
        private Literal sum;
        private ValueExprEvaluationException typeError;

        public SumAggregate(Sum operator) {
            super(operator);
            this.sum = vf.createLiteral("0", XMLSchema.INTEGER);
            this.typeError = null;
        }

        public void processAggregate(BindingSet s) throws QueryEvaluationException {
            if(this.typeError == null) {
                Value v = this.evaluate(s);
                if(this.distinctValue(v)) {
                    if(v instanceof Literal) {
                        Literal nextLiteral = (Literal)v;
                        if(nextLiteral.getDatatype() != null && XMLDatatypeUtil.isNumericDatatype(nextLiteral.getDatatype())) {
                            this.sum = MathUtil.compute(this.sum, nextLiteral, MathExpr.MathOp.PLUS);
                        } else {
                            this.typeError = new ValueExprEvaluationException("not a number: " + v);
                        }
                    } else if(v != null) {
                        this.typeError = new ValueExprEvaluationException("not a number: " + v);
                    }
                }

            }
        }

        public Value getValue() throws ValueExprEvaluationException {
            if(this.typeError != null) {
                throw this.typeError;
            } else {
                return this.sum;
            }
        }
    }

    private class MaxAggregate extends Aggregate {
        private final ValueComparator comparator = new ValueComparator();
        private Value max = null;

        public MaxAggregate(Max operator) {
            super(operator);
        }

        public void processAggregate(BindingSet s) throws QueryEvaluationException {
            Value v = this.evaluate(s);
            if(this.distinctValue(v)) {
                if(this.max == null) {
                    this.max = v;
                } else if(this.comparator.compare(v, this.max) > 0) {
                    this.max = v;
                }
            }

        }

        public Value getValue() {
            return this.max;
        }
    }

    private class MinAggregate extends Aggregate {
        private final ValueComparator comparator = new ValueComparator();
        private Value min = null;

        public MinAggregate(Min operator) {
            super(operator);
        }

        public void processAggregate(BindingSet s) throws QueryEvaluationException {
            Value v = this.evaluate(s);
            if(this.distinctValue(v)) {
                if(this.min == null) {
                    this.min = v;
                } else if(this.comparator.compare(v, this.min) < 0) {
                    this.min = v;
                }
            }

        }

        public Value getValue() {
            return this.min;
        }
    }

    private class CountAggregate extends Aggregate {
        private long count = 0L;
        private final Set<BindingSet> distinctBindingSets;

        public CountAggregate(Count operator) {
            super(operator);
            if(operator.isDistinct() && this.getArg() == null) {
                this.distinctBindingSets = new HashSet();
            } else {
                this.distinctBindingSets = null;
            }

        }

        public void processAggregate(BindingSet s) throws QueryEvaluationException {
            if(this.getArg() != null) {
                Value value = this.evaluate(s);
                if(value != null && this.distinctValue(value)) {
                    ++this.count;
                }
            } else if(this.distinctBindingSet(s)) {
                ++this.count;
            }

        }

        protected boolean distinctBindingSet(BindingSet s) {
            return this.distinctBindingSets == null || this.distinctBindingSets.add(s);
        }

        public Value getValue() {
            return vf.createLiteral(Long.toString(this.count), XMLSchema.INTEGER);
        }
    }

    private abstract class Aggregate {
        private final Set<Value> distinctValues;
        private final ValueExpr arg;

        public Aggregate(AggregateOperatorBase operator) {
            this.arg = operator.getArg();
            if(operator.isDistinct()) {
                this.distinctValues = new HashSet();
            } else {
                this.distinctValues = null;
            }

        }

        public abstract Value getValue() throws ValueExprEvaluationException;

        public abstract void processAggregate(BindingSet var1) throws QueryEvaluationException;

        protected boolean distinctValue(Value value) {
            return this.distinctValues == null || this.distinctValues.add(value);
        }

        protected ValueExpr getArg() {
            return this.arg;
        }

        protected Value evaluate(BindingSet s) throws QueryEvaluationException {
            try {
                return evaluateValue(getArg(), s);
            } catch (ValueExprEvaluationException var3) {
                return null;
            }
        }
    }

    private class Entry {
        private BindingSet prototype;
        private BindingSet parentBindings;
        private Map<String, Aggregate> aggregates;

        public Entry(BindingSet prototype, BindingSet parentBindings, Group group)
                throws ValueExprEvaluationException, QueryEvaluationException
        {
            this.prototype = prototype;
            this.parentBindings = parentBindings;
            this.aggregates = new LinkedHashMap();
            Iterator i$ = group.getGroupElements().iterator();

            while(i$.hasNext()) {
                GroupElem ge = (GroupElem)i$.next();
                Aggregate create = this.create(ge.getOperator());
                if(create != null) {
                    this.aggregates.put(ge.getName(), create);
                }
            }

        }

        public BindingSet getPrototype() {
            return this.prototype;
        }

        public void addSolution(BindingSet bindingSet) throws QueryEvaluationException {
            Iterator i$ = this.aggregates.values().iterator();

            while(i$.hasNext()) {
                Aggregate aggregate = (Aggregate)i$.next();
                aggregate.processAggregate(bindingSet);
            }

        }

        public void bindSolution(QueryBindingSet sol) throws QueryEvaluationException {
            Iterator i$ = this.aggregates.keySet().iterator();

            while(i$.hasNext()) {
                String name = (String)i$.next();

                try {
                    Value ex = ((Aggregate)this.aggregates.get(name)).getValue();
                    if(ex != null) {
                        sol.setBinding(name, ex);
                    }
                } catch (ValueExprEvaluationException var5) {
                    ;
                }
            }

        }

        private Aggregate create(AggregateOperator operator)
                throws QueryEvaluationException
        {
            if (operator instanceof Count)
                return new CountAggregate((Count)operator);
            else if (operator instanceof Min)
                return new MinAggregate((Min)operator);
            else if (operator instanceof Max)
                return new MaxAggregate((Max)operator);
            else if (operator instanceof Sum)
                return new SumAggregate((Sum)operator);
            else if (operator instanceof Avg)
                return new AvgAggregate((Avg)operator);
            else if (operator instanceof Sample)
                return new SampleAggregate((Sample)operator);
            else if (operator instanceof GroupConcat)
                return new ConcatAggregate((GroupConcat)operator, parentBindings);
            else
                return null;
        }
    }
}
