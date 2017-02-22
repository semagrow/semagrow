package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class can be used to build a {@link QueryBlock} graph from a {@link TupleExpr}.
 * @author acharal
 */
public class QueryBlockBuilder extends AbstractQueryModelVisitor<RuntimeException> {

    private QueryBlock block;

    private Scope scope;

    private class Scope {

        private LinkedList<Map<String,Quantifier.Var>> vars;

        public Scope() {
            vars = new LinkedList<>();
        }

        public void add(Quantifier q) {

            Map<String, Quantifier.Var> quantifierVars =
                    q.getVariables().stream()
                            .collect(Collectors.toMap(v -> v.getName(), v -> v));

            vars.addFirst(quantifierVars);
        }

        public Scope extend(Quantifier q) {
            Scope s = new Scope();
            s.vars = new LinkedList<>(vars);
            s.add(q);
            return s;
        }

        public void pop(){
            if (!vars.isEmpty())
                vars.removeFirst();
        }

        public Optional<Quantifier.Var> getVar(String name) {
            for (Map<String, Quantifier.Var> m : vars) {
                Quantifier.Var v = m.get(name);
                if (v != null)
                    return Optional.of(v);
            }
            return Optional.empty();
        }

        public Scope empty() { return new Scope(); }
    }

    private QueryBlockBuilder() { this.scope = new Scope(); }

    private QueryBlockBuilder(Scope scope) { this.scope = scope; }

    public static QueryBlock build(TupleExpr e) {

        QueryBlockBuilder builder = new QueryBlockBuilder();

        e.visit(builder);

        assert builder.block != null;
        return builder.block;
    }

    public static QueryBlock build(TupleExpr e, Scope scope) {

        QueryBlockBuilder builder = new QueryBlockBuilder(scope);

        e.visit(builder);

        assert builder.block != null;
        return builder.block;
    }


    @Override
    public void meet(StatementPattern e) {
        block = new PatternBlock(e);
    }

    @Override
    public void meet(BindingSetAssignment e) {
        block = new BindingSetAssignmentBlock(e.getBindingNames(), e.getBindingSets());
    }

    @Override
    public void meet(Group e) {
        super.meet(e);

        GroupBlock groupBlock = new GroupBlock(block, e.getGroupBindingNames());

        for (GroupElem elem : e.getGroupElements())
            groupBlock.addAggregation(elem.getName(), elem.getOperator());

        block = groupBlock;
    }

    @Override
    public void meet(Distinct e) {
        super.meet(e);
        SelectBlock selectBlock = new SelectBlock();

        Quantifier q = selectBlock.addFromBlock(block);
        copyOutputVars(q, selectBlock);

        selectBlock.setDuplicateStrategy(OutputStrategy.ENFORCE);

        block = selectBlock;
    }

    @Override
    public void meet(Reduced e) {
        super.meet(e);
        SelectBlock selectBlock = new SelectBlock();

        Quantifier q = selectBlock.addFromBlock(block);
        copyOutputVars(q, selectBlock);

        // if Reduced is used by the query then it means that the
        // expected result can tolerate partial elimination of duplicates.
        selectBlock.setDuplicateStrategy(OutputStrategy.PERMIT);

        block = selectBlock;
    }

    @Override
    public void meet(Order e) {

        super.meet(e);

        SelectBlock selectBlock = new SelectBlock();

        Quantifier q = selectBlock.addFromBlock(block);

        copyOutputVars(q, selectBlock);


        //FIXME: check if there are ValueExpr that needs to be add as derived columns since Ordering must contain only variable names.
        org.semagrow.plan.Ordering ordering = new org.semagrow.plan.Ordering();

        for (OrderElem elem : e.getElements()) {

            String v = getVarName(elem.getExpr(), selectBlock);

            org.semagrow.plan.Order o  = elem.isAscending() ?
                    org.semagrow.plan.Order.ASCENDING :
                    org.semagrow.plan.Order.DESCENDING;

            ordering.appendOrdering(v, o);
        }

        selectBlock.setOrdering(ordering);

        block = selectBlock;
    }

    @Override
    public void meet(Slice e) {
        super.meet(e);

        SelectBlock selectBlock = new SelectBlock();

        Quantifier q = selectBlock.addFromBlock(block);

        copyOutputVars(q, selectBlock);

        if (e.hasLimit())
            selectBlock.setLimit(e.getLimit());

        if (e.hasOffset())
            selectBlock.setOffset(e.getOffset());

        block = selectBlock;
    }

    @Override
    public void meet(Projection e) {
        super.meet(e);

        SelectBlock selectBlock = new SelectBlock();

        Quantifier q = selectBlock.addFromBlock(block);

        Set<String> projectedVars =
                e.getProjectionElemList().getElements().stream()
                        .map(elem -> elem.getSourceName()).collect(Collectors.toSet());

        Set<String> vars = new HashSet<>(block.getOutputVariables());
        vars.removeAll(projectedVars);

        //FIXME: 
        if (!vars.isEmpty()) {
            boolean found = false;
            // for all the variables that are not projected check if there are referred to the scope.
            for (String v : vars) {
                Optional<Quantifier.Var> maybeVar = scope.getVar(v);
                if (maybeVar.isPresent()) {
                    Optional<Quantifier.Var> maybeVar2 = q.getVariable(v);
                    if (maybeVar2.isPresent()) {
                        found = true;
                        selectBlock.addPredicate(new InnerJoinPredicate(maybeVar.get(), maybeVar2.get()));
                    }
                }
            }

            if (found) {
                block = selectBlock;
                selectBlock = new SelectBlock();
                q = selectBlock.addFromBlock(block);
            }
        }

        for (ProjectionElem elem : e.getProjectionElemList().getElements()) {

            Optional<Quantifier.Var> v = q.getVariable(elem.getSourceName());

            if (v.isPresent())
                selectBlock.addProjection(elem.getTargetName(), v.get());
            else
                throw new RuntimeException("There is a bug in transforming Projection to QueryBlock!");

            projectedVars.add(elem.getSourceName());
        }

        block = selectBlock;
    }

    @Override
    public void meet(Extension e) {
        super.meet(e);

        SelectBlock selectBlock = new SelectBlock();

        Quantifier q = selectBlock.addFromBlock(block);

        for (Quantifier.Var v : q.getVariables())
            selectBlock.addProjection(v.getName(), v);

        for (ExtensionElem elem : e.getElements()) {
            if (elem.getExpr() instanceof AggregateOperator) {
                // ignore aggregate operators. These are included in the GroupBlock
            } else {
                ValueExpr ext = VarReplacer.process(elem.getExpr(), q);
                selectBlock.addProjection(elem.getName(), ext);
            }
        }

        block = selectBlock;
    }

    @Override
    public void meet(Filter e) {
        super.meet(e);

        SelectBlock selectBlock = new SelectBlock();

        Quantifier q = selectBlock.addFromBlock(block);

        copyOutputVars(q, selectBlock);

        SubQueryHandler handler = new SubQueryHandler(e.getCondition());

        if (handler.hasSubQuery()) {

            QueryBlock subBlock = QueryBlockBuilder.build(handler.getSubQuery(), scope.extend(q));

            Quantifier subQ = handler.isExistential() ?
                      selectBlock.addExistentialBlock(subBlock)
                    : selectBlock.addUniversalBlock(subBlock);

            if (handler.hasLhs()) {

                ValueExpr lhs = VarReplacer.process(handler.getLhs(), q);

                Optional<Quantifier.Var> v  = subQ.getVariables().stream().findFirst();

                if (v.isPresent()) {
                    ValueExpr valueArg = new Compare(lhs, v.get(), handler.getOperator());
                    selectBlock.addPredicate(new ThetaJoinPredicate(valueArg));
                }
            }

        } else {

            ValueExpr ee = VarReplacer.process(e.getCondition(), q);
            selectBlock.addPredicate(new ThetaJoinPredicate(ee));
        }

        block = selectBlock;
    }

    @Override
    public void meet(Union e) {
        e.getLeftArg().visit(this);
        QueryBlock leftBlock = block;
        e.getRightArg().visit(this);
        QueryBlock rightBlock = block;
        block = new UnionBlock(leftBlock, rightBlock);
    }

    @Override
    public void meet(Intersection e) {
        e.getLeftArg().visit(this);
        QueryBlock leftBlock = block;
        e.getRightArg().visit(this);
        QueryBlock rightBlock = block;
        block = new IntersectionBlock(leftBlock, rightBlock);
    }

    @Override
    public void meet(Difference e) {
        e.getLeftArg().visit(this);
        QueryBlock leftBlock = block;
        e.getRightArg().visit(this);
        QueryBlock rightBlock = block;
        block = new MinusBlock(leftBlock, rightBlock);
    }

    @Override
    public void meet(Join e) {

        SelectBlock selectBlock = new SelectBlock();

        e.getLeftArg().visit(this);

        Quantifier f = selectBlock.addFromBlock(block);
        copyOutputVars(f, selectBlock);

        Set<String> vars = new HashSet<>(block.getOutputVariables());

        e.getRightArg().visit(this);

        Quantifier t = selectBlock.addFromBlock(block);
        vars.retainAll(block.getOutputVariables());

        for (Quantifier.Var v : t.getVariables()) {
            if (!vars.contains(v.getName()))
                selectBlock.addProjection(v.getName(), v);
        }

        for (String v : vars) {
            Optional<Quantifier.Var> from = f.getVariable(v);
            Optional<Quantifier.Var> to   = t.getVariable(v);
            if (from.isPresent() && to.isPresent())
                selectBlock.addPredicate(new InnerJoinPredicate(from.get(), to.get()));
        }

        block = selectBlock;
    }

    @Override
    public void meet(LeftJoin e) {
        SelectBlock selectBlock = new SelectBlock();

        e.getLeftArg().visit(this);

        Quantifier f = selectBlock.addFromBlock(block);
        copyOutputVars(f, selectBlock);

        Set<String> vars = new HashSet<>(block.getOutputVariables());

        e.getRightArg().visit(this);

        Quantifier t = selectBlock.addFromBlock(block);
        vars.retainAll(block.getOutputVariables());

        for (Quantifier.Var v : t.getVariables()) {
            if (!vars.contains(v.getName()))
                selectBlock.addProjection(v.getName(), v);
        }

        Optional<ValueExpr> cond = Optional.empty();

        if (e.hasCondition()) {
            ValueExpr c = VarReplacer.process(e.getCondition(), f);
            c = VarReplacer.process(c, t);
            cond = Optional.of(c);
        }

        for (String v : vars) {
            Optional<Quantifier.Var> from = f.getVariable(v);
            Optional<Quantifier.Var> to   = t.getVariable(v);

            if (from.isPresent() && to.isPresent())
                selectBlock.addPredicate(new LeftJoinPredicate(from.get(), to.get(), cond));
        }

        block = selectBlock;
    }

    private void copyOutputVars(Quantifier q, SelectBlock b) {
        for (Quantifier.Var v : q.getVariables())
            b.addProjection(v.getName(), v);
    }

    public String getVarName(ValueExpr expr, SelectBlock block) {
        if (expr instanceof Var) {
            Var v = (Var) expr;
            if (!v.hasValue())
                return v.getName();
        }
        String v = createNewVarName();
        block.addProjection(v, expr);
        return v;
    }

    public String createNewVarName() {
        return "_new_" + UUID.randomUUID().toString().replaceAll("-", "_");
    }

    /* Replaces */
    static private class VarReplacer extends AbstractQueryModelVisitor<RuntimeException> {

        private Quantifier q;

        public VarReplacer(Quantifier q) { this.q = q; }

        @Override
        public void meet(Var var) {
            if (!var.hasValue()) {
                Optional<Quantifier.Var> qvar = q.getVariable(var.getName());
                if (qvar.isPresent())
                    var.replaceWith(qvar.get());
            }
        }

        public static <X extends QueryModelNode> X process(X node, Quantifier q) {
            VarReplacer replacer = new VarReplacer(q);
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

    private class SubQueryHandler {

        private SubQueryValueOperator op;

        private boolean negated;

        public SubQueryHandler(ValueExpr e) {
            process(e, false);
        }

        protected void process(ValueExpr e, boolean neg) {
            if (e instanceof Not)
                process(((Not)e).getArg(), !neg);
            else if (e instanceof SubQueryValueOperator)
            {
                op = (SubQueryValueOperator) e;
                negated = neg;
            }
        }

        public TupleExpr getSubQuery() {
            if (op != null)
                return op.getSubQuery();
            else
                return null;
        }

        public boolean hasSubQuery() { return getSubQuery() != null; }

        public boolean hasLhs() { return getLhs() != null; }

        public ValueExpr getLhs() {

            if (op instanceof CompareSubQueryValueOperator) {
                return ((CompareSubQueryValueOperator) op).getArg();
            }

            return null;
        }

        public boolean isExistential() {

            boolean exist = true;

            if (op == null)
                throw new IllegalArgumentException("Expression has not subquery");

            if (op instanceof CompareAll)
                exist = false;

            if (negated)
                exist = !exist;

            return exist;
        }

        public Compare.CompareOp getOperator() {
            Compare.CompareOp compareOp = null;

            if (op instanceof CompareAll)
                compareOp = ((CompareAll)op).getOperator();
            else if (op instanceof CompareAny)
                compareOp = ((CompareAny)op).getOperator();
            else if (op instanceof In)
                compareOp = Compare.CompareOp.EQ;

            if (compareOp != null) {
                if (negated)
                    return negateOp(compareOp);
                else
                    return compareOp;
            }
            else
                throw new RuntimeException();
        }

        private CompareOp negateOp(CompareOp op) {
            switch (op) {
                case EQ : return CompareOp.NE;
                case NE : return CompareOp.EQ;
                case LE : return CompareOp.GT;
                case GE : return CompareOp.LT;
                case GT : return CompareOp.LE;
                case LT : return CompareOp.GE;
                default : throw new RuntimeException();
            }
        }
    }

}