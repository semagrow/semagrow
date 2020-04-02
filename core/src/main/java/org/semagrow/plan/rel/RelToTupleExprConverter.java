package org.semagrow.plan.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.semagrow.plan.rel.sparql.SparqlStdOperatorTable;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by antonis on 10/7/2017.
 */
public class RelToTupleExprConverter implements ReflectiveVisitor {

    private final ReflectUtil.MethodDispatcher<TupleExpr> dispatcher;

    public RelToTupleExprConverter() {
        dispatcher = ReflectUtil.createMethodDispatcher(TupleExpr.class, this, "visit",
                RelNode.class);
    }

    /** Dispatches a call to the {@code visit(Xxx e)} method where {@code Xxx}
     * most closely matches the runtime type of the argument. */
    protected TupleExpr dispatch(RelNode e) {
        return dispatcher.invoke(e);
    }

    public TupleExpr visitChild(int i, RelNode e) {
        return dispatch(e);
    }

    public TupleExpr convertQuery(RelRoot query) {
        TupleExpr e = dispatch(query.rel);
        return new org.eclipse.rdf4j.query.algebra.QueryRoot(e);
    }

    public TupleExpr visit(RelNode e) {
        throw new AssertionError("Need to implement " + e.getClass().getName());
    }

    public TupleExpr visit(Aggregate node) {


        TupleExpr input = visitChild(0, node.getInput());

        if (node.getAggCallList().isEmpty() &&
            node.getRowType().equals(node.getInput().getRowType()))
        {
            return new org.eclipse.rdf4j.query.algebra.Distinct(input);
        }


        org.eclipse.rdf4j.query.algebra.Group g = new org.eclipse.rdf4j.query.algebra.Group(input);
        RelDataType t  = node.getInput().getRowType();
        ImmutableBitSet groupSet = node.getGroupSet();
        for (RelDataTypeField f : t.getFieldList()) {
            if (groupSet.get(f.getIndex())) {
                g.addGroupBindingName(f.getName());
            }
        }

        org.eclipse.rdf4j.query.algebra.Extension e = new org.eclipse.rdf4j.query.algebra.Extension(g);
        for (AggregateCall call: node.getAggCallList()) {
            org.eclipse.rdf4j.query.algebra.AbstractAggregateOperator aggregate = convertAggregateCall(call, node.getRowType());
            g.addGroupElement(new org.eclipse.rdf4j.query.algebra.GroupElem(call.getName(), aggregate));
            e.addElement(new org.eclipse.rdf4j.query.algebra.ExtensionElem(aggregate, call.getName()));
        }
        return e;
    }

    private org.eclipse.rdf4j.query.algebra.AbstractAggregateOperator convertAggregateCall(AggregateCall call, RelDataType rowtype) {
        ValueExpr arg = null;
        if (!(call.getArgList().isEmpty())) {
            int index = call.getArgList().get(0);
            String varname = rowtype.getFieldList().get(index).getName();
            arg = new Var(varname);
        }
        if (call.getAggregation().equals(SqlStdOperatorTable.AVG)) {
            return new org.eclipse.rdf4j.query.algebra.Avg(arg, call.isDistinct());
        }
        if (call.getAggregation().equals(SqlStdOperatorTable.COUNT)) {
            return new org.eclipse.rdf4j.query.algebra.Count(arg, call.isDistinct());
        }
        if (call.getAggregation().equals(SqlStdOperatorTable.MAX)) {
            return new org.eclipse.rdf4j.query.algebra.Max(arg, call.isDistinct());
        }
        if (call.getAggregation().equals(SqlStdOperatorTable.MIN)) {
            return new org.eclipse.rdf4j.query.algebra.Min(arg, call.isDistinct());
        }
        if (call.getAggregation().equals(SqlStdOperatorTable.SUM)) {
            return new org.eclipse.rdf4j.query.algebra.Sum(arg, call.isDistinct());
        }
        if (call.getAggregation().equals(SparqlStdOperatorTable.GROUPCONCAT)) {
            return new org.eclipse.rdf4j.query.algebra.GroupConcat(arg, call.isDistinct());
        }
        if (call.getAggregation().equals(SparqlStdOperatorTable.SAMPLE)) {
            return new org.eclipse.rdf4j.query.algebra.Sample(arg, call.isDistinct());
        }
        return null;
    }

    public TupleExpr visit(Filter node) {
        //ValueExpr condition = rexToValueExprConverter.convertRexNode(node.getCondition(), node.getRowType());
        //return new Filter(convertRelNode(node.getInput()), condition);
        return visitChild(0, node.getInput());
    }

    public TupleExpr visit(Join node) {

        List<Integer> leftKeys = Lists.newArrayList();
        List<Integer> rightKeys = Lists.newArrayList();

        RexNode rest = RelOptUtil.splitJoinCondition(
                node.getLeft(),
                node.getRight(),
                node.getCondition(),
                leftKeys,
                rightKeys,
                null);

        TupleExpr leftArg = visitChild(0, node.getLeft());
        TupleExpr rightArg = visitChild(1, node.getRight());

        TupleExpr out = null;

        RexToValueExprConverter.Context ctx = new NodeContext(node);

        if (node.getJoinType() == JoinRelType.INNER) {
             out = new org.eclipse.rdf4j.query.algebra.Join(leftArg, rightArg);
             if (!rest.isAlwaysTrue()) {
                 ValueExpr cond = RexToValueExprConverter.convertExpression(rest, ctx);
                 out = new org.eclipse.rdf4j.query.algebra.Filter(out, cond);
             }
        } else if (node.getJoinType() == JoinRelType.LEFT) {
            if (rest.isAlwaysTrue())
                out = new org.eclipse.rdf4j.query.algebra.LeftJoin(leftArg, rightArg);
            else {
                ValueExpr cond = RexToValueExprConverter.convertExpression(rest, ctx);
                out = new org.eclipse.rdf4j.query.algebra.LeftJoin(leftArg, rightArg, cond);
            }
        } else {
            throw new AssertionError();
        }
        return out;
    }

    public TupleExpr visit(Project node) {

        TupleExpr input = visitChild(0, node.getInput());

        org.eclipse.rdf4j.query.algebra.ProjectionElemList lst = new org.eclipse.rdf4j.query.algebra.ProjectionElemList();

        /* create a mapping (id -> varable name) for the inner variable list */

        Map<Integer, String> innerVariablesMap = new HashMap<>();
        List innerFieldList = node.getInput().getRowType().getFieldList();
        for (int i = 0 ; i<innerFieldList.size() ; i++) {
            Object field = innerFieldList.get(i);
            if (field instanceof RelDataTypeField) {
                innerVariablesMap.put(i, ((RelDataTypeField) field).getName());
            }
        }

        Map<String, String> renamings = new HashMap<>();
        List<String> nullFieldList = new ArrayList();
        List<ExtensionElem> extensionElemList = new ArrayList<>();

        for (Pair<RexNode,String> n : node.getNamedProjects()) {

            RexNode project = n.getKey();

            if (project instanceof RexLiteral) { //FIXME
                if (((RexLiteral) project).getValue() == null) {
                    nullFieldList.add(n.getValue());
                }
            }

            if (project instanceof RexCall) { //FIXME
                ExtensionElem elem = new ExtensionElem(createFunction(project, innerVariablesMap), n.getValue());
                extensionElemList.add(elem);
            }

            if (project instanceof RexInputRef) { //FIXME
                Var var = createExtensionVar(project, innerVariablesMap);
                if (!(var.getName().equals(n.getValue()))) {
                    ExtensionElem elem = new ExtensionElem(var, n.getValue());
                    extensionElemList.add(elem);
                    renamings.put(n.getValue(), var.getName());
                }

            }
        }
        if (!(extensionElemList.isEmpty())) {
            input = new Extension(input, extensionElemList);
        }
        for (String field: node.getRowType().getFieldNames()) {
            if (!nullFieldList.contains(field)) {
                if (renamings.containsKey(field)) {
                    lst.addElement(new org.eclipse.rdf4j.query.algebra.ProjectionElem(renamings.get(field), field));
                } else {
                    lst.addElement(new org.eclipse.rdf4j.query.algebra.ProjectionElem(field));
                }
            }
        }

        return new org.eclipse.rdf4j.query.algebra.Projection(input, lst);
    }

    public ValueExpr createFunction(RexNode node, Map<Integer, String> innerVariablesMap) {

        if (node instanceof RexCall) {
            FunctionCall call = new FunctionCall();
            call.setURI(((RexCall) node).getOperator().getName());
            List<ValueExpr> args = new ArrayList<>();
            for (RexNode n: ((RexCall) node).getOperands()) {
                args.add(createFunction(n, innerVariablesMap));
            }
            call.setArgs(args);
            return call;
        }
        if (node instanceof RexInputRef) {
            return createExtensionVar(node, innerVariablesMap);
        }
        if (node instanceof RexLiteral) {
            RexToValueExprConverter converter = new RexToValueExprConverter(null);
            Value value = converter.convertLiteral((RexLiteral) node);
            return new ValueConstant(value);
        }
        return null;
    }

    public Var createExtensionVar(RexNode node, Map<Integer, String> innerVariablesMap) {
        if (node instanceof RexInputRef) {
            int index = ((RexInputRef) node).getIndex();
            return new Var(innerVariablesMap.get(index));
        }
        return null;
    }

    public TupleExpr visit(Intersect node) {
        List<TupleExpr> inputs = Lists.newArrayList();
        for (int i = 0; i < node.getInputs().size(); i++) {
            inputs.add(visitChild(i, node.getInput(i)));
        }

        TupleExpr expr = inputs.get(0);
        for (int i = 1; i<node.getInputs().size(); i++) {
            expr = new org.eclipse.rdf4j.query.algebra.Intersection(expr, inputs.get(i));
        }
        return expr;
    }

    public TupleExpr visit(Minus node) {

        List<TupleExpr> inputs = Lists.newArrayList();
        for (int i = 0; i < node.getInputs().size(); i++) {
            inputs.add(visitChild(i, node.getInput(i)));
        }

        TupleExpr expr = inputs.get(0);
        for (int i = 1; i<node.getInputs().size(); i++) {
            expr = new org.eclipse.rdf4j.query.algebra.Difference(expr, inputs.get(i));
        }

        return expr;
    }

    public TupleExpr visit(Union node) {
        List<TupleExpr> inputs = Lists.newArrayList();
        for (int i = 0; i < node.getInputs().size(); i++) {
            inputs.add(visitChild(i, node.getInput(i)));
        }

        TupleExpr expr = inputs.get(0);
        for (int i = 1; i<node.getInputs().size(); i++) {
            expr = new org.eclipse.rdf4j.query.algebra.Union(expr, inputs.get(i));
        }

        return expr;
    }

    public TupleExpr visit(Sort node) {

        TupleExpr input = visitChild(0, node.getInput());

        if (RelOptUtil.isOrder(node)) {
            RelDataType rowType = node.getRowType();
            List<RelFieldCollation> fieldCols = node.getCollation().getFieldCollations();
            List<org.eclipse.rdf4j.query.algebra.OrderElem> orderElems = Lists.newArrayList();
            for (RelFieldCollation fieldCol : fieldCols) {
                String name = rowType.getFieldList().get(fieldCol.getFieldIndex()).getName();
                Var var = new Var(name);
                orderElems.add(new org.eclipse.rdf4j.query.algebra.OrderElem(var,
                        fieldCol.direction == RelFieldCollation.Direction.ASCENDING));
            }
            input = new org.eclipse.rdf4j.query.algebra.Order(input, orderElems);
        }

        if (RelOptUtil.isLimit(node)) {
            long fetch  = node.fetch == null ? -1 : ((RexLiteral)node.fetch).getValueAs(BigDecimal.class).longValue();
            long offset = node.offset == null ? -1 : ((RexLiteral)node.offset).getValueAs(BigDecimal.class).longValue();
            input = new org.eclipse.rdf4j.query.algebra.Slice(input, offset, fetch);
        }

        return input;
    }

    public TupleExpr visit(Values node) {

        RelDataType rowType = node.getRowType();

        BindingSetAssignment out = new BindingSetAssignment();
        out.setBindingNames(Sets.newHashSet(rowType.getFieldNames()));

        ImmutableList.Builder<BindingSet> bindingSets = ImmutableList.builder();

        for (ImmutableList<RexLiteral> tuple : node.getTuples())
        {
            int i = 0;
            QueryBindingSet bs = new QueryBindingSet();
            for (RexLiteral l : tuple) {
                String name = rowType.getFieldList().get(i).getName();
                Value v = RexToValueExprConverter.convertLiteral(l);
                bs.addBinding(name, v);
                i++;
            }
            bindingSets.add(bs);
        }

        out.setBindingSets(bindingSets.build());

        return out;
    }

    public TupleExpr visit(StatementScan node) {

        List<Var> vars = new ArrayList<>();

        for (RelDataTypeField field : node.getRowType().getFieldList()) {
            vars.add(new Var(field.getName()));
        }

        org.eclipse.rdf4j.query.algebra.StatementPattern p =
                new org.eclipse.rdf4j.query.algebra.StatementPattern(vars.get(0), vars.get(1), vars.get(2), vars.get(3));

        return p;
    }


    class NodeContext implements RexToValueExprConverter.Context {

        private RelNode node;

        public NodeContext(RelNode node) {
            this.node = node;
        }

        @Override
        public ValueExpr field(int ordinal) {
            RelDataType type = this.node.getRowType();
            String name = type.getFieldList().get(ordinal).getName();
            return new Var(name);
        }
    }
}
