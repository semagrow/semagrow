package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.local.LocalSite;
import org.semagrow.plan.*;
import org.semagrow.plan.operators.BindJoin;
import org.semagrow.plan.operators.CrossProduct;
import org.semagrow.plan.operators.HashJoin;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.selector.Site;
import org.semagrow.util.CombinationIterator;
import org.semagrow.util.PartitionedSet;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author acharal
 */
public class SelectBlock extends AbstractQueryBlock {

    // the name of the variables that will be appear in the output
    // this is a mapping between the source name of the variable and the target name
    private Map<String, ValueExpr> outputVars;

    // graph of queryblocks connected with Predicates
    private Collection<Predicate> predicates;

    private Collection<Quantifier> quantifiers;

    private Optional<Long> limit = Optional.empty();

    private Optional<Long> offset = Optional.empty();

    public SelectBlock() {
        outputVars = new HashMap<>();
        predicates = new LinkedList<>();
        quantifiers = new LinkedList<>();
    }

    public Set<String> getOutputVariables() { return outputVars.keySet(); }

    public Optional<ValueExpr> getLocal(String name) {
        return Optional.ofNullable(outputVars.get(name));
    }

    public Map<String,ValueExpr> getLocals() { return outputVars; }

    public boolean hasDuplicates() {

        // if it is enforced by this block then the output do not contain any duplicates.
        if (getDuplicateStrategy() == OutputStrategy.ENFORCE) {
            return false;
        } else if (getDuplicateStrategy() == OutputStrategy.PRESERVE) {

            // if it preserved the strategy of the constituent blocks then
            // check whether there is a single EACH quantifier and that
            // quantifier has not duplicates. In that case, this block has
            // also no duplicates.

            Collection<Quantifier> qs = getQuantifiers().stream().filter(q -> q.isFrom()).collect(Collectors.toList());

            if (qs.size() == 1)
                return !qs.stream().noneMatch(q -> q.getBlock().hasDuplicates());
        }

        // in any other case we cannot guarantee that there will be no duplicates.
        return true;
    }

    public void setOrdering(Ordering o) {
        DataProperties props = getOutputDataProperties();
        props.setOrdering(o);
        setOutputProperties(props);
    }

    public void setLimit(Long l) { limit = Optional.of(l); }

    public void setOffset(Long l) { offset = Optional.of(l); }

    @Override
    public <X extends Exception> void visitChildren(QueryBlockVisitor<X> visitor) throws X {
        for (Quantifier q : quantifiers)
            q.getBlock().visit(visitor);
    }

    public Quantifier addFromBlock(QueryBlock block) {
        Quantifier q = new Quantifier(block, Quantifier.Quantification.EACH);
        quantifiers.add(q);
        q.setParent(this);
        return q;
    }

    public Quantifier addExistentialBlock(QueryBlock block) {
        Quantifier q = new Quantifier(block, Quantifier.Quantification.ANY);
        quantifiers.add(q);
        q.setParent(this);
        return q;
    }

    public Quantifier addUniversalBlock(QueryBlock block) {
        Quantifier q = new Quantifier(block, Quantifier.Quantification.ALL);
        quantifiers.add(q);
        q.setParent(this);
        return q;
    }

    public void removeQuantifier(Quantifier q) {
        if (q.getParent().equals(this))
            this.quantifiers.remove(q);
    }

    public void moveQuantifier(Quantifier q) {
        if (!q.getParent().equals(this)) {
            SelectBlock b = (SelectBlock)q.getParent();
            b.quantifiers.remove(q);
            q.setParent(this);
            this.quantifiers.add(q);
        }
    }

    public Collection<Quantifier> getQuantifiers() { return quantifiers; }

    public Predicate addPredicate(Predicate p) {
        predicates.add(p);
        return p;
    }

    public void removePredicate(Predicate p) {
        predicates.remove(p);
    }

    public void movePredicate(Predicate p) {
        this.predicates.add(p);
    }

    public Collection<Predicate> getPredicates() { return predicates; }

    public Collection<Predicate> getPredicates(Quantifier q) {
        return predicates.stream()
                .filter(p -> p.getEL().contains(q))
                .collect(Collectors.toList());
    }

    public void addProjection(String var, ValueExpr expr) {
        if (var == null)
            throw new IllegalArgumentException("Quantified variable " + var.toString() + " not a member of the block");

        outputVars.put(var, expr);
    }

    public Collection<Plan> getPlans(CompilerContext context) {
        // find subsets of connected quantifier quantified with EACH
        // for each subset apply the join enumeration
        // join the disconnected subsets using cross-join
        Collection<Set<Quantifier>> qList = findConnectedQuantifiers();

        Collection<Pair<Collection<Quantifier>, Collection<Plan>>> planList = new LinkedList<>();

        PredicateApplicator applicator = new PredicateApplicator(context);
        for (Collection<Quantifier> qc : qList) {

            DPPredicateEnumerator<Quantifier, Plan> enumerator = new DPPredicateEnumerator<>(applicator);

            // filter out from qc Quantifier with Quantification other than EACH
            Collection<Plan> plans = enumerator.enumerate(qc);

            if (!plans.isEmpty())
                planList.add(new Pair<>(qc, plans));
            else
                return Collections.emptyList();
        }

        if (planList.size() == 1) {
            Collection<Plan> completePlans = applicator.finalize(planList.iterator().next());
            context.prune(completePlans);
            return completePlans;
        } else {
            Optional<Pair<Collection<Quantifier>, Collection<Plan>>> result =  planList.stream().reduce(applicator::combine);
            if (result.isPresent()) {
                Collection<Plan> completePlans = new LinkedList<>(result.get().getSecond());
                context.prune(completePlans);
                return completePlans;
            } else {
                return Collections.emptyList();
            }
        }
    }

    /**
     * Returns a list of sets of quantifiers that are connected directly or indirectly
     * via a predicate.
     * @return a collection of sets of quantifiers
     */
    protected Collection<Set<Quantifier>> findConnectedQuantifiers() {

        Collection<Quantifier> qc = getQuantifiers();

        PartitionedSet<Quantifier> partitions = new PartitionedSet<Quantifier>(qc);

        for (Predicate p : getPredicates()) {
            Collection<Quantifier> qq = p.getEL();
            Quantifier qPrev = null;
            for (Quantifier q : qq) {
                if (qPrev == null)
                    qPrev = q;
                else {
                    partitions.union(qPrev, q);
                }
            }
        }
        return partitions.getPartitions();
    }

    /**
     * Computes the equivalent classes of {@code Quantifier} variables. Two quantifier
     * variables are equivalent if there is an {@code InnerJoinPredicate} that connects them.
     * @return a partioned set of quantifier variables
     */
    protected PartitionedSet<Quantifier.Var> computeEquivalentVars() {

        Collection<Quantifier.Var> qv = getQuantifiers().stream()
                .flatMap( q -> q.getVariables().stream())
                .collect(Collectors.toList());

        PartitionedSet<Quantifier.Var> equivalentClasses = new PartitionedSet<>(qv);

        for (Predicate p : getPredicates()) {
            if (p instanceof InnerJoinPredicate) {
                InnerJoinPredicate ijp = (InnerJoinPredicate)p;
                equivalentClasses.union(ijp.getFrom(), ijp.getTo());
            }
        }

        return equivalentClasses;
    }

    private PartitionedSet<Quantifier.Var> equivalentVars;

    private Boolean mustComputeEquivalentClasses = true;

    public PartitionedSet<Quantifier.Var> getEquivalentVariables() {

        if (mustComputeEquivalentClasses) {
            equivalentVars = computeEquivalentVars();
            mustComputeEquivalentClasses = false;
        }

        return equivalentVars;
    }

    public Set<Quantifier.Var> getEquivalentVariables(Quantifier.Var v) {
        PartitionedSet<Quantifier.Var> partitions = getEquivalentVariables();
        return partitions.getEquals(v);
    }

    public String repName(Quantifier.Var v) {
        PartitionedSet<Quantifier.Var> classes = getEquivalentVariables();
        return classes.rep(v).getName();
    }

    public Quantifier.Var repVar(Quantifier.Var v) {
        PartitionedSet<Quantifier.Var> classes = getEquivalentVariables();
        return classes.rep(v);
    }

    class RepReplacer extends AbstractQueryModelVisitor<RuntimeException> {

        public void meetOther(QueryModelNode node) {
            if (node instanceof Quantifier.Var) {
                Quantifier.Var var = repVar((Quantifier.Var)node);
                node.replaceWith(var);
            }
            super.meetOther(node);
        }
    }

    // class VarRenamer extends AbstractPlanVisitor<RuntimeException> {
    class VarRenamer extends AbstractQueryModelVisitor<RuntimeException> {

        private Map<String,String> theta;

        public VarRenamer(Map<String,String> theta) { this.theta = theta; }

        public void meet(Var v) {
            if (!v.isConstant()) {
                String name = theta.get(v.getName());
                if (name != null) {
                    v.setName(name);
                }
            }
        }

        public void meet(Projection projection){
            projection.getProjectionElemList().visit(this);
            //projection.getArg().visit(this);
            //super.meet(projection);
        }

        public void meet(ProjectionElem elem) {
            String newTargetName = theta.get(elem.getTargetName());
            if (newTargetName != null) {
                elem.setTargetName(newTargetName);
            }
        }
    }

    static class QuantifierToVarTransformer extends AbstractQueryModelVisitor<RuntimeException> {

        public void meetOther(QueryModelNode node) {
            if (node instanceof Quantifier.Var) {
                String varname = ((Quantifier.Var)node).getName();
                node.replaceWith(new Var(varname));
            }
            super.meetOther(node);
        }


        public static <X extends QueryModelNode> X process(X node) {
            QuantifierToVarTransformer replacer = new QuantifierToVarTransformer();
            X clone = (X) node.clone();
            DummyParent<X> parent = new DummyParent<>(clone);
            clone.visit(replacer);
            return parent.getChild();
        }

        static private class DummyParent<X extends QueryModelNode> extends AbstractQueryModelNode {

            private X child;

            public DummyParent(X child) {
                this.child = child;
                this.child.setParentNode(this);
            }

            public X getChild() { return child; }

            public void replaceChildNode(QueryModelNode current, QueryModelNode replacement) {
                if (current == child) {
                    child = (X) replacement;
                }
            }

            @Override
            public <Y extends Exception> void visit(QueryModelVisitor<Y> queryModelVisitor) throws Y {
                child.visit(queryModelVisitor);
            }
        }

    }


    /**
     * A class that knows how to compose Plans out of Quantifiers by applying predicates
     */
    class PredicateApplicator implements DPPredicateEnumerator.PlanGenerator<Quantifier,Plan> {

        private CompilerContext context;

        public PredicateApplicator(CompilerContext context) { this.context = context; }

        /**
         * Creates a {@link Plan} for {@link Quantifier} {@code q} by applying all
         * possible predicates.
         * @param q the Quantifier
         * @return A pair that contains the set of quantifier that have been involved
         *         to the plan generation and a set of alternative equivalent {@link Plan}s
         */
        public Pair<Collection<Quantifier>, Collection<Plan>> access(Quantifier q) {
            // FIXME: What happens if q is Quantification.ALL or Quantification.ANY

            Collection<Quantifier> k = Collections.singleton(q);

            // apply thetajoinpredicates that contain only this quantifier.
            Collection<Predicate> predicates = getPredicates().stream()
                    .filter(p -> isRelevant(p, k))
                    .collect(Collectors.toList());

            Collection<Plan> plans = q.getBlock().getPlans(context);

            // rename each plan variable with the representative variable of its equivalent class
            Map<String,String> varMap = q.getVariables().stream()
                    .collect(Collectors.toMap(v -> v.getName(), v -> repName(v)));
            final VarRenamer renamer = new VarRenamer(varMap);
            plans.stream().forEach(p -> p.visit(renamer));


            if (!predicates.isEmpty()) {
                plans = apply(plans, predicates);
            }

            return new Pair<>(k, plans);
        }

        /**
         * Creates a collection of alternative {@link Plan}s by combining two plans (one from each collection) using
         * a set of applicable predicates.
         * @param left a collection of plans that reside on the left hand side of the predicates. Attached to the plans
         *             the is a collection of the quantifier that are used to create those plans.
         * @param right a collection of plans that reside on the right hand side of the predicates. Attached to the plans
         *             the is a collection of the quantifier that are used to create those plans.
         * @return a collection of combined plans together with the quantifier that are used to create those plans.
         */
        public Pair<Collection<Quantifier>, Collection<Plan>> compose(Pair<Collection<Quantifier>, Collection<Plan>> left,
                                                                      Pair<Collection<Quantifier>, Collection<Plan>> right)
        {
            Collection<Quantifier> all = new HashSet<>(left.getFirst().size() + right.getFirst().size());
            all.addAll(left.getFirst());
            all.addAll(right.getFirst());

            if (left.getSecond().isEmpty() || right.getSecond().isEmpty())
                return new Pair<>(all, new LinkedList<>());

            Collection<Predicate> relevant = getPredicates().stream().
                    filter(p -> isRelevant(p, left.getFirst(), right.getFirst())).collect(Collectors.toList());

            // find the applicable predicates
            Collection<Predicate> predicates = relevant.stream()
                    .filter(p -> isApplicable(p, left.getFirst(), right.getFirst()))
                    .collect(Collectors.toList());

            if (!predicates.isEmpty()) {
                Collection<Plan> plans = apply(left.getSecond(), right.getSecond(), predicates);
                return new Pair<>(all, plans);
            } /*else if (relevant.isEmpty()) {
                // FIXME: it is not always valid to crossProduct plans if there are no applicable Predicates.
                //        separate this functionality with "combine" unless we get any better idea.
                // if there is no relevant predicates then it is a cross product
                Collection<Plan> plans = crossProduct(left, right);
                return new Pair<>(all, plans);
            }*/ else {
                // there are relevant predicates but not applicable (yet?)
                // so produce an empty collection of plans to signal that those
                // quantifiers cannot be composed yet.
                // quantifiers cannot be composed yet.
                return new Pair<>(all, new LinkedList<>());
            }
        }

        public Pair<Collection<Quantifier>, Collection<Plan>> combine(Pair<Collection<Quantifier>, Collection<Plan>> left,
                                                                      Pair<Collection<Quantifier>, Collection<Plan>> right)
        {
            Collection<Quantifier> all = new HashSet<>(left.getFirst().size() + right.getFirst().size());
            all.addAll(left.getFirst());
            all.addAll(right.getFirst());
            Collection<Plan> plans = crossProduct(left, right);
            return new Pair<>(all, plans);
        }

        boolean isRelevant(Predicate p, Collection<Quantifier> input) {

            // check whether each variable in p has an variable in input that is equivalent

            return p.getVariables().stream()
                    .allMatch( v -> containsEquivVar(v, input));
        }

        boolean isRelevant(Predicate p, Collection<Quantifier> from, Collection<Quantifier> to) {
            return (p.getVariables().stream().anyMatch(v -> containsEquivVar(v,from)) &&
                    p.getVariables().stream().anyMatch(v -> containsEquivVar(v,to)));
        }

        boolean containsEquivVar(Quantifier.Var v, Collection<Quantifier> input) {
            return getEquivalentVariables(v).stream()
                    .map(Quantifier.Var::getQuantifier)
                    .anyMatch(input::contains);
        }

        boolean isApplicable(Predicate p, Collection<Quantifier> from, Collection<Quantifier> to) {

            Collection<Quantifier> all = new HashSet<>(from.size() + to.size());
            all.addAll(from);
            all.addAll(to);

            if (!isRelevant(p, all))
                return false;

            // check that at least one variable from `from` and one from `to` participates
            // in the predicate.
            // This assumes that no predicate is delayed but every predicate are applied
            // as soon as they become applicable. This might concern especially expensive
            // ThetaJoinPredicates that might be delayed.
            // More correctly,  plans may record which predicates are applied in order
            // to avoid reapplication.
            if (!p.getVariables().stream().anyMatch(v -> containsEquivVar(v,from)) ||
                !p.getVariables().stream().anyMatch(v -> containsEquivVar(v,to))) {
                return false;
            }

            if (p instanceof BinaryPredicate) {
                BinaryPredicate bp = (BinaryPredicate)p;
                if (!containsEquivVar(bp.getFrom(), from) || !containsEquivVar(bp.getTo(), to))
                    return false;
            }

            if (p instanceof LeftJoinPredicate)
            {
                BinaryPredicate bp = (BinaryPredicate)p;

                if (!from.contains(bp.getFrom().getQuantifier()) ||
                        !to.contains(bp.getTo().getQuantifier()) )
                    return false;

                if (!all.containsAll(bp.getEEL()))
                    return false;
            }

            return true;
        }

        Collection<Plan> apply(Collection<Plan> plans, Collection<Predicate> predicates) {

            Collection<Predicate> pp = predicates.stream()
                    .filter(p -> !(p instanceof BinaryPredicate) && (p instanceof ThetaJoinPredicate))
                    .collect(Collectors.toList());

            return plans.stream()
                    .flatMap( p ->  apply(p, pp) )
                    .collect(Collectors.toList());
        }

        Stream<Plan> apply(Plan p, Collection<Predicate> predicates) {

            final RepReplacer repReplacer = new RepReplacer();

            Stream<ValueExpr> exprs = predicates.stream()
                    .filter(pred -> pred instanceof ThetaJoinPredicate)
                    .map( pred -> (ThetaJoinPredicate)pred)
                    .map(pred -> {
                        ValueExpr e = pred.asExpr().clone();
                        e.visit(repReplacer);
                        return QuantifierToVarTransformer.process(e);
                    });

            //FIXME: replace Quantifier.Var in e with regular variables (?)
            for (ValueExpr e : exprs.collect(Collectors.toList()))
                p = context.asPlan(new Filter(p, e));

            return Stream.of(p);
        }


        Collection<Plan> apply(Collection<Plan> left, Collection<Plan> right, Collection<Predicate> validPredicates) {

            if (validPredicates.isEmpty())
                return Collections.emptyList();

            Collection<Predicate> joinPredicates = validPredicates.stream()
                    .filter(p -> p instanceof InnerJoinPredicate || p instanceof ThetaJoinPredicate).collect(Collectors.toList());

            Collection<Predicate> leftjoinPredicates = validPredicates.stream()
                    .filter(p -> p instanceof LeftJoinPredicate).collect(Collectors.toList());

            assert joinPredicates.isEmpty() || leftjoinPredicates.isEmpty();

            if (!joinPredicates.isEmpty()) {
                return applyJoins(left, right, joinPredicates);
            } else if (!leftjoinPredicates.isEmpty()) {
                return applyLeftJoins(left, right, leftjoinPredicates);
            } else {
                throw new RuntimeException("Is it possible to have inner and outer join at the same time?");
            }
        }

        private Collection<Plan> applyJoins(Collection<Plan> left, Collection<Plan> right, Collection<Predicate> joinPredicates)
        {
            Collection<Plan> plans = new LinkedList<>();

            for (Plan leftPlan : left) {
                for (Plan rightPlan : right) {
                    applyJoins(leftPlan, rightPlan, joinPredicates, plans);
                }
            }
            return plans;
        }

        private void applyJoins(Plan left, Plan right, Collection<Predicate> joinPredicates, Collection<Plan> outPlans) {

            JoinImplGenerator[] impls = new JoinImplGenerator[]{
                    new BindJoinImplGenerator()
                    , new RemoteJoinImplGenerator()
                    //, new HashJoinImplGenerator()
            };

            for (JoinImplGenerator joinImpl : impls)
            {
                Collection<Plan> jj = joinImpl.apply(left, right, joinPredicates, context);

                outPlans.addAll(jj);
            }
        }

        private Collection<Plan> applyLeftJoins(Collection<Plan> left, Collection<Plan> right, Collection<Predicate> joinPredicates) {
            Collection<Plan> plans = new LinkedList<>();


            for (Plan leftPlan : left) {
                for (Plan rightPlan : right) {
                    applyLeftJoins(leftPlan, rightPlan, joinPredicates, plans);
                }
            }
            return plans;
        }


        private void applyLeftJoins(Plan left, Plan right, Collection<Predicate> joinPredicates, Collection<Plan> outPlans) {
            LeftJoinImplGenerator[] impls = new LeftJoinImplGenerator[]{
                    new RemoteLeftJoinImplGenerator(),
                    new NLLeftJoinImplGenerator()
            };

            for (LeftJoinImplGenerator joinImpl : impls)
            {
                Collection<Plan> jj = joinImpl.apply(left, right, joinPredicates, context);
                outPlans.addAll(jj);
            }
        }

        @Override
        public Iterator<Pair<Collection<Quantifier>, Collection<Quantifier>>> enumerate(Collection<Quantifier> tt) {
            return new PairSubsetIterator<>(tt);
        }

        public Collection<Plan> finalize(Pair<Collection<Quantifier>, Collection<Plan>> plans) {

            return plans.getSecond().stream()
                    .flatMap( p -> finalize(plans.getFirst(), p) )
                    .collect(Collectors.toList());
        }

        private Stream<Plan> finalize(Collection<Quantifier> qs, Plan p) {

            return applyExtensions(qs, p)
                    .flatMap( this::project )
                    .flatMap( pp -> {
                            if (getInterestingProperties().getStructureProperties().isEmpty())
                                return Stream.of(pp);
                            else
                                return getInterestingProperties().getStructureProperties().stream()
                                    .flatMap(dataProps -> {
                                        RequestedPlanProperties props = new RequestedPlanProperties();
                                        props.setDataProperties(dataProps);
                                        return context.enforceProps(pp, props).stream();
                                    });
                    })
                    .flatMap( this::deduplicate )
                    .flatMap( this::limitOutput );
        }

        /**
         * Inserts an operator that deduplicates the result
         * @param p
         * @return
         */
        public Stream<Plan> deduplicate(Plan p) {

            if (getDuplicateStrategy() == OutputStrategy.ENFORCE && p.hasDuplicates()) {

                Stream<Plan> output = Stream.of(context.asPlan(new Distinct(p)));

                RequestedDataProperties groupingProps = RequestedDataProperties.forGrouping(getOutputVariables());

                if (groupingProps.isCoveredBy(p.getProperties().getDataProperties())) {
                    output = Stream.concat(output, Stream.of(context.asPlan(new Reduced(p))));
                }

                return output;

            } else {
                return Stream.of(p);
            }
        }

        public Stream<Plan> limitOutput(Plan p) {
            if (limit.isPresent() || offset.isPresent()) {
                Slice slice = new Slice(p);
                limit.ifPresent(slice::setLimit);
                offset.ifPresent(slice::setOffset);
                return Stream.of(context.asPlan(slice));
            } else {
                return Stream.of(p);
            }
        }

        public Stream<Plan> applyExtensions(Collection<Quantifier> qs, Plan p) {
            Set<String> planVars = p.getOutputVariables();

            Collection<Map.Entry<String, ValueExpr>> exprs = getLocals().entrySet().stream()
                    .filter(entry -> !(entry.getValue() instanceof Quantifier.Var))
                    .filter(entry ->
                         qs.containsAll(QuantifierCollector.process(entry.getValue()).stream()
                                 .map(Quantifier.Var::getQuantifier).collect(Collectors.toSet()))
                    )
                    .filter(entry -> !planVars.contains(entry.getKey()))
                    .collect(Collectors.toList());

            if (exprs.isEmpty()) {
                //find applicable extensions.
                return Stream.of(p);
            } else {
                Extension ext = new Extension(p, exprs.stream()
                        .map(entry -> {
                            ValueExpr e = entry.getValue().clone();
                            e.visit(new RepReplacer());
                            e = QuantifierToVarTransformer.process(e);
                            return new ExtensionElem(e, entry.getKey());
                        })
                        .collect(Collectors.toList()));

                return Stream.of(context.asPlan(ext));
            }
        }

        public Stream<Plan> project(Plan p) {

            Collection<Map.Entry<String,String>>
                projections = getLocals().entrySet().stream()
                    .flatMap(entry -> {
                        String sourceName = entry.getKey();
                        if (entry.getValue() instanceof Quantifier.Var) {
                            Quantifier.Var qv = (Quantifier.Var)entry.getValue();
                            sourceName = repName(qv);
                        }
                        if (p.getOutputVariables().contains(sourceName))
                            return Stream.of(new HashMap.SimpleEntry<>(entry.getKey(), sourceName));
                        else
                            return Stream.empty();
                    })
                    .collect(Collectors.toList());

            boolean allIdentical = projections.stream().allMatch( e -> e.getKey().equals(e.getValue()) );
            boolean projectionNeeded = true;
            if (allIdentical) {
                Set<String> vars = projections.stream().map(e -> e.getKey()).collect(Collectors.toSet());
                if (vars.containsAll(p.getOutputVariables())) {
                    projectionNeeded = false;
                }
            }

            if (projectionNeeded) {
                List<ProjectionElem> elements = projections.stream().map(entry -> new ProjectionElem(entry.getValue(), entry.getKey()))
                        .collect(Collectors.toList());
                Plan pp = context.asPlan(new Projection(p, new ProjectionElemList(elements)));
                return Stream.of(pp);
            }

            return Stream.of(p);
        }

        public Collection<Plan> crossProduct(Pair<Collection<Quantifier>, Collection<Plan>> left,
                                             Pair<Collection<Quantifier>, Collection<Plan>> right)
        {
            return left.getSecond().stream()
                    .flatMap(p1 -> right.getSecond().stream()
                            .flatMap(p2 -> crossProduct(left.getFirst(), p1, right.getFirst(), p2)
                )).collect(Collectors.toList());
        }

        private Stream<Plan> crossProduct(Collection<Quantifier> first, Plan p1, Collection<Quantifier> first1, Plan p2) {

            if (p1.getProperties().getSite().equals(p2.getProperties().getSite()))
                return Stream.of(context.asPlan(new CrossProduct(p1, p2)));
            else {
                RequestedPlanProperties props = new RequestedPlanProperties();
                props.setSite(LocalSite.getInstance());
                Collection<Plan> pp1 = context.enforceProps(p1, props);
                Collection<Plan> pp2 = context.enforceProps(p2, props);
                return pp1.stream().flatMap(left -> pp2.stream().flatMap(right -> crossProduct(first, left, first1, right)));
            }
        }

        public void prune(Collection<Plan> plans) {
            context.prune(plans);
        }


        public class BindJoinImplGenerator implements JoinImplGenerator {

            @Override
            public Collection<Plan> apply(Plan p1, Plan p2, Collection<Predicate> preds, CompilerContext context) {

                Collection<Plan> plans = new LinkedList<>();

                Collection<Predicate> innerPreds = preds.stream()
                        .filter(p -> (p instanceof InnerJoinPredicate))
                        .collect(Collectors.toList());

                Collection<Predicate> otherPreds = preds.stream()
                        .filter(p -> !(p instanceof InnerJoinPredicate))
                        .collect(Collectors.toList());

                RequestedPlanProperties props = new RequestedPlanProperties();
                props.setSite(LocalSite.getInstance());

                Collection<Plan> pp1c = context.enforceProps(p1, props);

                if (isBindable(p2, p1.getOutputVariables())) {

                    Collection<Plan> pp2c = context.enforceProps(p2, props);

                    if (!innerPreds.isEmpty()) {
                        for (Plan pp1 : pp1c) {
                            for (Plan pp2 : pp2c) {
                                BindJoin b = new BindJoin(pp1, pp2);
                                // find remaining thetajoin predicates and also apply then in b
                                plans.add(context.asPlan(b));
                                // apply thetajoin predicates in pp2 and then bindjoin (pp1, filter(pp2))
                            }
                        }
                        plans = PredicateApplicator.this.apply(plans, otherPreds);
                    }

                    if (!otherPreds.isEmpty()) {
                        Collection<Plan> p2f  = PredicateApplicator.this.apply(p2, otherPreds).collect(Collectors.toList());
                        Collection<Plan> pp2fc = context.enforceProps(p2f, props);

                        for (Plan pp1 : pp1c) {
                            for (Plan pp2 : pp2fc) {
                                BindJoin b = new BindJoin(pp1, pp2);
                                // find remaining thetajoin predicates and also apply then in b
                                plans.add(context.asPlan(b));
                                // apply thetajoin predicates in pp2 and then bindjoin (pp1, filter(pp2))
                            }
                        }
                    }
                }

                return plans;
            }

            private boolean isBindable(Plan p, Set<String> bindingNames) {

                IsBindableVisitor v = new IsBindableVisitor();

                p.visit(v);
                if( v.condition ) {
                    Site s = p.getProperties().getSite();
                    if (s.isRemote()) {
                        return s.getCapabilities().acceptsBindings(p, bindingNames);
                    }
                }

                return v.condition;
            }


            private class IsBindableVisitor extends AbstractPlanVisitor<RuntimeException> {
                boolean condition = false;

                @Override
                public void meet(Union union) {
                    union.getLeftArg().visit(this);

                    if (condition)
                        union.getRightArg().visit(this);
                }

                @Override
                public void meet(Join join) {
                    condition = false;
                }

                @Override
                public void meet(Plan e) {
                    if (e.getProperties().getSite().isRemote())
                        condition = true;
                    else
                        e.getArg().visit(this);
                }

                @Override
                public void meet(SourceQuery query) {
                    condition = true;
                }
            }

        }

        public class RemoteJoinImplGenerator implements JoinImplGenerator {

            @Override
            public Collection<Plan> apply(Plan p1, Plan p2, Collection<Predicate> preds, CompilerContext context) {

                Collection<Plan> plans = new LinkedList<>();

                PlanProperties props1 = p1.getProperties();
                PlanProperties props2 = p2.getProperties();

                if (props1.getSite().equals(props2.getSite())
                        && props1.getSite().isRemote())
                {
                    Join b = new Join(p1, p2);
                    plans.add(context.asPlan(b));
                }

                preds = preds.stream().filter(p -> !(p instanceof InnerJoinPredicate)).collect(Collectors.toList());

                return PredicateApplicator.this.apply(plans, preds);
            }
        }

        public class HashJoinImplGenerator implements JoinImplGenerator {

            public Collection<Plan> apply(Plan p1, Plan p2, Collection<Predicate> preds, CompilerContext context) {
                Collection<Plan> plans = new LinkedList<>();

                RequestedPlanProperties props = new RequestedPlanProperties();
                props.setSite(LocalSite.getInstance());

                Collection<Plan> pp1c = context.enforceProps(p1, props);
                Collection<Plan> pp2c = context.enforceProps(p2, props);

                for (Plan pp1 : pp1c) {
                    for (Plan pp2 : pp2c) {
                        HashJoin b = new HashJoin(pp1, pp2);
                        // find remaining thetajoin predicates and also apply then in b
                        plans.add(context.asPlan(b));

                        // apply thetajoin predicates in pp2 and then bindjoin (pp1, filter(pp2))
                    }
                }

                preds = preds.stream().filter(p -> !(p instanceof InnerJoinPredicate)).collect(Collectors.toList());

                return PredicateApplicator.this.apply(plans, preds);
            }
        }

        public class RemoteLeftJoinImplGenerator implements LeftJoinImplGenerator {

            @Override
            public Collection<Plan> apply(Plan p1, Plan p2, Collection<Predicate> preds, CompilerContext context) {

                Collection<Plan> plans = new LinkedList<>();

                PlanProperties props1 = p1.getProperties();
                PlanProperties props2 = p2.getProperties();

                if (props1.getSite().equals(props2.getSite())
                        && props1.getSite().isRemote())
                {
                    LeftJoin b = new LeftJoin(p1, p2);
                    plans.add(context.asPlan(b));
                }

                preds = preds.stream().filter(p -> !(p instanceof LeftJoinPredicate)).collect(Collectors.toList());

                return PredicateApplicator.this.apply(plans, preds);
            }
        }


        public class NLLeftJoinImplGenerator implements LeftJoinImplGenerator {

            @Override
            public Collection<Plan> apply(Plan p1, Plan p2, Collection<Predicate> preds, CompilerContext context) {

                Collection<Plan> plans = new LinkedList<>();

                RequestedPlanProperties props = new RequestedPlanProperties();
                props.setSite(LocalSite.getInstance());

                Collection<Plan> pp1c = context.enforceProps(p1, props);
                Collection<Plan> pp2c = context.enforceProps(p2, props);

                for (Plan pp1 : pp1c) {
                    for (Plan pp2 : pp2c) {
                        LeftJoin b = new LeftJoin(pp1, pp2);
                        // find remaining thetajoin predicates and also apply then in b
                        plans.add(context.asPlan(b));

                        // apply thetajoin predicates in pp2 and then bindjoin (pp1, filter(pp2))
                    }
                }

                preds = preds.stream().filter(p -> !(p instanceof LeftJoinPredicate)).collect(Collectors.toList());

                return PredicateApplicator.this.apply(plans, preds);
            }
        }
    }

    public interface JoinImplGenerator {

        Collection<Plan> apply(Plan p1, Plan p2, Collection<Predicate> preds, CompilerContext context);

    }

    public interface LeftJoinImplGenerator {

        Collection<Plan> apply(Plan p1, Plan p2, Collection<Predicate> preds, CompilerContext context);

    }


    private static <T> Iterable<Pair<Collection<T>, Collection<T>>> pairs(Collection<T> s) {
        return new PairSubsetIterator<T>(s);
    }

    static public class SubsetsIterator<T> implements Iterator<Collection<T>> {

        private final Collection<T> elements;
        private int k = 0;

        private Iterator<Collection<T>> current;

        public SubsetsIterator(Collection<T> elements) {
            this.elements = elements;
            k = 0;
        }

        public SubsetsIterator(Collection<T> elements, int k)
        {
            this(elements);
            this.k = k - 1;
            assert k > 0;
            assert k <= elements.size();
        }

        public boolean hasNext() {
            return k <= elements.size();
        }

        public Collection<T> next() {

            if (k > this.elements.size())
                return null;

            if (current == null || !current.hasNext()) {
                k++;

                if (k == this.elements.size()) {
                    k++;
                    return elements;
                }

                current = new CombinationIterator<>(k, elements);
            }

            return current.next();
        }
    }

    static public class PairSubsetIterator<T> implements Iterator<Pair<Collection<T>, Collection<T>>>, Iterable<Pair<Collection<T>,Collection<T>>>  {
        private Collection<T> items;

        private Iterator<Collection<T>> outer;
        private Iterator<Collection<T>> inner;

        private Collection<T> outerCurrent;
        private Pair<Collection<T>,Collection<T>> n;


        public PairSubsetIterator(Collection<T> items) {
            this.items = items;
            init();
        }

        public void init() {
            if (items.size() < 2) {
                n = null;
            } else {
                outer = new SubsetsIterator<T>(items, 2);
                n = getNext();
            }
        }

        @Override
        public boolean hasNext() {
            return n != null;
        }

        @Override
        public Pair<Collection<T>, Collection<T>> next() {
            Pair<Collection<T>,Collection<T>> nn = n;
            n = getNext();
            return nn;
        }


        protected Pair<Collection<T>, Collection<T>> getNext()
        {
            while (true)
            {

                if (inner == null || !inner.hasNext()) {

                    if (!outer.hasNext())
                        return null;
                    else
                    {
                        // get next outerCurrent, init inner
                        outerCurrent = outer.next();
                        inner = new SubsetsIterator<T>(outerCurrent);
                    }
                }

                if (inner.hasNext()) {
                    Collection<T> i = inner.next();
                    Pair<Collection<T>,Collection<T>> p =  getPair(outerCurrent, i);

                    if (p != null)
                        return p;
                }
            }

        }

        private Pair<Collection<T>,Collection<T>> getPair(Collection<T> full, Collection<T> part)
        {
            Collection<T> s = new HashSet<>(full);
            s.removeAll(part);
            if (s.isEmpty())
                return null;
            else
                return new Pair<>(s, part);
        }

        @Override
        public Iterator<Pair<Collection<T>, Collection<T>>> iterator() {
            return this;
        }
    }

}
