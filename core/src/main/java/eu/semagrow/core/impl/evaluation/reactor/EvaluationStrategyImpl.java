package eu.semagrow.core.impl.evaluation.reactor;

import eu.semagrow.core.eval.BindingSetOps;
import eu.semagrow.core.impl.evaluation.util.BindingSetOpsImpl;
import eu.semagrow.core.impl.evaluation.util.QueryEvaluationUtil;
import eu.semagrow.core.eval.EvaluationStrategy;
import eu.semagrow.core.impl.evaluation.IterationPublisher;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverImpl;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil;
import org.eclipse.rdf4j.query.algebra.evaluation.util.OrderComparator;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;
import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.stream.GroupedStream;

import java.util.*;


/**
 * Created by antonis on 26/3/2015.
 */
public class EvaluationStrategyImpl implements EvaluationStrategy {

    private ValueFactory vf;
    private org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy evalStrategy;
    protected BindingSetOps bindingSetOps = BindingSetOpsImpl.getInstance();

    private Dispatcher dispatcher;

    public EvaluationStrategyImpl(TripleSource tripleSource) {
        vf = tripleSource.getValueFactory();
        evalStrategy = new org.eclipse.rdf4j.query.algebra.evaluation.impl.SimpleEvaluationStrategy(tripleSource, new FederatedServiceResolverImpl());
        Environment.initializeIfEmpty();
        //dispatcher = new MDCAwareDispatcher(Environment.dispatcher(Environment.THREAD_POOL));
    }

    public EvaluationStrategyImpl(TripleSource tripleSource, Dataset dataset) {
        vf = tripleSource.getValueFactory();
        evalStrategy = new org.eclipse.rdf4j.query.algebra.evaluation.impl.SimpleEvaluationStrategy(tripleSource,dataset,new FederatedServiceResolverImpl());
        Environment.initializeIfEmpty();
        //dispatcher = new MDCAwareDispatcher(Environment.dispatcher(Environment.THREAD_POOL));
    }

    public Publisher<BindingSet> evaluate(TupleExpr expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        //return RxReactiveStreams.toPublisher(evaluateReactorInternal(expr, bindings));;
        return evaluateReactorInternal(expr, bindings);
                //.subscribeOn(new MDCAwareDispatcher(Environment.dispatcher(Environment.THREAD_POOL)));
    }

    public boolean isTrue(ValueExpr expr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException
    {
        return evalStrategy.isTrue(expr, bindings);
    }

    public Value evaluate(ValueExpr expr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException
    {
        return evalStrategy.evaluate(expr, bindings);
    }

    public Value evaluateValue(ValueExpr expr, BindingSet bindings)
            throws ValueExprEvaluationException, QueryEvaluationException
    {
        return evaluate(expr, bindings);
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
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(BindingSetAssignment expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return Streams.from(expr.getBindingSets())
                .filter((b) -> bindingSetOps.agreesOn(bindings, b))
                .map((b) -> bindingSetOps.merge(bindings, b));
    }

    public Stream<BindingSet> evaluateReactorInternal(ExternalSet expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(ZeroLengthPath expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(ArbitraryLengthPath expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Filter expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        QueryBindingSet scopeBindings = new QueryBindingSet(bindings);

        return evaluateReactorInternal(expr.getArg(), bindings)
                .filter((b) -> {
                    try {
                        return this.isTrue(expr.getCondition(), b);
                    } catch (QueryEvaluationException /*| ValueExprEvaluationException */ e) {
                        return false;
                    }
                });
    }

    public Stream<BindingSet> evaluateReactorInternal(Projection expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorInternal(expr.getArg(), bindings)
                .map((b) -> QueryEvaluationUtil.project(expr.getProjectionElemList(), b, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Extension expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorInternal(expr.getArg(), bindings)
                .flatMap((b) -> {
                    try {
                        return Streams.just(QueryEvaluationUtil.extend(this, expr.getElements(), b));
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
                this.evaluateReactorInternal(expr.getRightArg(), bindings))
                    .subscribeOn(new MDCAwareDispatcher(Environment.dispatcher(Environment.WORK_QUEUE)))
                    .dispatchOn(new MDCAwareDispatcher(Environment.dispatcher(Environment.SHARED)));
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
                }).subscribeOn(new MDCAwareDispatcher(Environment.dispatcher(Environment.WORK_QUEUE)));
    }

    public Stream<BindingSet> evaluateReactorInternal(LeftJoin expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Stream<BindingSet> r = evaluateReactorInternal(expr.getRightArg(), bindings);

        Set<String> joinAttributes = expr.getLeftArg().getBindingNames();
        joinAttributes.retainAll(expr.getRightArg().getBindingNames());

        return evaluateReactorInternal(expr.getLeftArg(), bindings)
                .flatMap((b) -> {
                    try {
                        return this.evaluateReactorInternal(expr.getRightArg(), b).defaultIfEmpty(b);
                    } catch (Exception e) {
                        return Streams.fail(e);
                    }
                }).subscribeOn(new MDCAwareDispatcher(Environment.dispatcher(Environment.WORK_QUEUE)))
                .dispatchOn(new MDCAwareDispatcher(Environment.dispatcher(Environment.SHARED)));
    }

    public Stream<BindingSet> evaluateReactorInternal(Group expr, BindingSet bindings)
            throws QueryEvaluationException
    {
        Set<String> groupByBindings = expr.getGroupBindingNames();

        Stream<BindingSet> s = evaluateReactorInternal(expr.getArg(), bindings);

        Stream<GroupedStream<BindingSet, BindingSet>> g = s.groupBy((b) -> bindingSetOps.project(groupByBindings, b, bindings));

        //return g.flatMap((gs) -> Streams.just(gs.key()));
        return g.flatMap((gs) -> aggregate(gs, expr, bindings));
    }

    public Stream<BindingSet> aggregate(GroupedStream<BindingSet, BindingSet> g, Group expr, BindingSet parentBindings) {
        BindingSet k = g.key();

        try {
            Entry e = new Entry(k, parentBindings, expr);
            Stream<Entry> s = g.reduce( e, (e1, b) -> {
                try { e1.addSolution(b);
                    return e1;
                }
                catch(Exception x) {
                    return null;
                }
            });
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
        OrderComparator cmp = new OrderComparator(evalStrategy, expr, vcmp);
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
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }

    public Stream<BindingSet> evaluateReactorInternal(Intersection expr, BindingSet bindings)
            throws QueryEvaluationException {
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(Difference expr, BindingSet bindings)
            throws QueryEvaluationException {
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }


    public Stream<BindingSet> evaluateReactorInternal(Service expr, BindingSet bindings)
            throws QueryEvaluationException {
        return fromIteration(evalStrategy.evaluate(expr, bindings));
    }

    protected <T> Stream<T> fromIteration(Iteration<T, ? extends Exception> it) {
        return Streams.wrap(new IterationPublisher(it));
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

        public Aggregate(AbstractAggregateOperator operator) {
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
