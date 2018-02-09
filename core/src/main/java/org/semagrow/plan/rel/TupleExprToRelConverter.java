package org.semagrow.plan.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.*;
import org.apache.calcite.rel.logical.*;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Sample;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.plan.rel.logical.LogicalService;
import org.semagrow.plan.rel.logical.LogicalStatementScan;
import org.semagrow.plan.rel.schema.AbstractDataset;
import org.semagrow.plan.rel.schema.RelOptDataset;
import org.semagrow.plan.rel.schema.RelOptDatasetImpl;
import org.semagrow.plan.rel.type.RDFTypes;
import org.semagrow.plan.rel.type.RdfDataTypeFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by angel on 30/6/2017.
 */
public class TupleExprToRelConverter {

    private RelOptCluster cluster;
    private RdfDataTypeFactory typeFactory;
    private RdfRexBuilder rexBuilder;
    private ValueExprToRexConverter exprConverter;
    private CatalogReader catalogReader;

    private final Map<CorrelationId, DeferredLookup> mapCorrelToDeferred =
            new HashMap<>();

    public TupleExprToRelConverter(RelOptCluster cluster, CatalogReader catalogReader) {
        this.cluster = Preconditions.checkNotNull(cluster);
        this.typeFactory = (RdfDataTypeFactory) cluster.getTypeFactory();
        this.rexBuilder = new RdfRexBuilder(this.typeFactory);
        this.exprConverter = new ValueExprToRexConverter(cluster);
        this.catalogReader = Preconditions.checkNotNull(catalogReader);
    }

    public RelRoot convertQuery(QueryRoot queryNode) {

        RelNode result = convertQueryRecursively(Scopes.empty(), queryNode.getArg());

        RelCollation collation = RelCollations.EMPTY;

        if (isOrdered(result))
            collation = requiredCollation(result);

        return RelRoot.of(result, result.getRowType(), SqlKind.SELECT)
                .withCollation(collation);
    }


    private RelCollation requiredCollation(RelNode r) {
        if (r instanceof Sort) {
            RelCollation col =  ((Sort) r).collation;
            if (col.equals(RelCollations.EMPTY))
                return requiredCollation(((Sort)r).getInput());
            else
                return col;
        }

        if (r instanceof Project) {
            return requiredCollation(((Project) r).getInput());
        }

        throw new AssertionError();
    }

    private boolean isOrdered(RelNode r) {
        if (r instanceof Sort) {
            return true;
        } else if (r instanceof Project)
            return isOrdered(((Project)r).getInput());
        else
            return false;
    }

    private RelNode convertQueryRecursively(Scope scope, TupleExpr query) {

        RelNode n = convertTupleExpr(scope, query);

        n = injectCorrelatedJoinConditions(scope, n, n.getRowType().getFieldNames());

        return n;
    }

    private RelNode convertTupleExpr(Scope scope, TupleExpr query) {

        if (query instanceof Union)
            return convertUnion(scope, (Union)query);
        else if (query instanceof Difference)
            return convertDifference(scope, (Difference)query);
        else if (query instanceof Intersection)
            return convertIntersection(scope, (Intersection)query);
        else if (query instanceof Projection)
            return convertProject(scope, (Projection)query);
        else if (query instanceof Extension)
            return convertExtension(scope, (Extension)query);
        else if (query instanceof Filter)
            return convertFilter(scope, (Filter)query);
        else if (query instanceof Distinct)
            return convertDistinct(scope, (Distinct)query);
        else if (query instanceof Group)
            return convertAgg(scope, (Group)query);
        else if (query instanceof Join)
            return convertJoin(scope, (Join)query);
        else if (query instanceof LeftJoin)
            return convertLeftJoin(scope, (LeftJoin)query);
        else if (query instanceof StatementPattern)
            return convertPattern(scope, (StatementPattern)query);
        else if (query instanceof BindingSetAssignment)
            return convertBindingSetAssignment(scope, (BindingSetAssignment)query);
        else if (query instanceof Order)
            return convertOrder(scope, (Order)query, null, null);
        else if (query instanceof Slice)
            return convertLimit(scope, (Slice)query);
        else if (query instanceof org.eclipse.rdf4j.query.algebra.Service)
            return convertService(scope, (org.eclipse.rdf4j.query.algebra.Service)query);
        else
            return null;
    }

    private RelNode convertPattern(Scope scope, StatementPattern query)
    {
        //RelOptDataset dataset = RelOptDatasetImpl.create(null, new AbstractDataset());

        RelOptDataset dataset = catalogReader.getDataset();

        Var[] vars = new Var[] {
                query.getSubjectVar(),
                query.getPredicateVar(),
                query.getObjectVar(),
                query.getContextVar() };

        ImmutableList.Builder<RexNode> conditions = ImmutableList.builder();
        ImmutableBitSet.Builder columns = ImmutableBitSet.builder();

        Map<String, Integer> varNamesMap = new HashMap<>();

        for (int i = 0; i < vars.length; i++) {
            if (vars[i] != null) {
                if (vars[i].isConstant()) {
                    assert vars[i].getValue() != null;
                    List<RexNode> operands = new ArrayList<>();
                    RexLiteral lit = makeLiteral(vars[i].getValue());
                    operands.add(rexBuilder.makeInputRef(lit.getType(), i));
                    operands.add(lit);
                    conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,operands));
                } else {
                    if (varNamesMap.containsKey(vars[i].getName())) {
                        // must include an equality of varNamesMap.get(vars[i].getName()) and i
                        List<RexNode> operands = new ArrayList<>();
                        operands.add(rexBuilder.makeInputRef(RDFTypes.IRI_TYPE, varNamesMap.get(vars[i].getName())));
                        operands.add(rexBuilder.makeInputRef(RDFTypes.IRI_TYPE, i));
                        conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,operands));
                    } else {
                        columns.set(i);
                        varNamesMap.put(vars[i].getName(), i);
                    }
                }
            }
        }

        return LogicalStatementScan.create(cluster, dataset)
                .projectAndFilter(
                        columns.build(),
                        ImmutableList.copyOf(varNamesMap.keySet()),
                        RexUtil.composeConjunction(rexBuilder,conditions.build(),false),
                        RelFactories.LOGICAL_BUILDER.create(cluster,null));
    }

    private RelNode convertDifference(Scope scope, Difference node) {
        RelNode left = convertTupleExpr(scope, node.getLeftArg());
        RelNode right = convertTupleExpr(scope, node.getRightArg());

        return LogicalMinus.create(ImmutableList.of(left,right), true);
    }

    private RelNode convertUnion(Scope scope, Union node) {

        RelNode left = convertTupleExpr(scope, node.getLeftArg());
        RelNode right = convertTupleExpr(scope, node.getRightArg());

        RelDataType unionType = mergeUnionType(left.getRowType(), right.getRowType());

        left = projectOrNull(left, unionType);
        right = projectOrNull(right, unionType);

        return LogicalUnion.create(ImmutableList.of(left,right), true);
    }

    private RelNode convertService(Scope scope, org.eclipse.rdf4j.query.algebra.Service node) {
        RelNode input = convertTupleExpr(scope, node.getArg());
        RexNode ref = convertExpression(Scopes.delegate(scope), node.getServiceRef());
        return LogicalService.create(input, ref, node.isSilent());
    }

    private RelNode projectOrNull(RelNode n, RelDataType unionType) {

        RelDataType inputType = n.getRowType();
        ImmutableList.Builder<String> fields = ImmutableList.builder();
        ImmutableList.Builder<RexNode> exprs = ImmutableList.builder();

        boolean hasNulls = false;

        for (RelDataTypeField f : unionType.getFieldList()) {
            fields.add(f.getName());
            RelDataTypeField ff = inputType.getField(f.getName(), false, false);

            if (ff != null) {
                exprs.add(rexBuilder.makeInputRef(n, ff.getIndex()));
            } else {
                hasNulls = true;
                exprs.add(rexBuilder.constantNull());
            }
        }

        if (hasNulls)
            return RelFactories.DEFAULT_PROJECT_FACTORY
                .createProject(n, exprs.build(), fields.build());
        else
            return n;
    }

    private RelDataType mergeUnionType(RelDataType t1, RelDataType t2) {
        List<String> common = Lists.newArrayList(t1.getFieldNames());
        common.retainAll(t2.getFieldNames());
        List<String> all = Lists.newArrayList(t1.getFieldNames());
        all.addAll(t2.getFieldNames());
        List<String> notcommon = Lists.newArrayList(all);
        notcommon.removeAll(common);

        RelDataTypeFactory.FieldInfoBuilder type = typeFactory.builder();
        for (String name : common) {
            type.add(t1.getField(name, false, false));
        }

        for (String name : notcommon) {
            RelDataTypeField f = t1.getField(name, false, false);
            if (f == null) {
                f = t2.getField(name, false, false);
            }
            assert f != null;
            type.add(f);
        }

        return type.build();
    }

    private RelNode convertIntersection(Scope scope, Intersection node) {
        RelNode left  = convertTupleExpr(scope, node.getLeftArg());
        RelNode right = convertTupleExpr(scope, node.getRightArg());

        return LogicalIntersect.create(ImmutableList.of(left,right), true);
    }

    private RelNode convertJoin(Scope scope, Join node) {
        RelNode leftRel  = convertTupleExpr(scope, node.getLeftArg());
        Scope leftScope =  Scopes.of(leftRel, scope);
        RelNode rightRel = convertTupleExpr(leftScope,  node.getRightArg());

        List<String> columnNames = deriveNaturalJoinColumnList(leftRel.getRowType(), rightRel.getRowType());

        RexNode condition = convertUsing(leftRel.getRowType(), rightRel.getRowType(), columnNames);

        return createJoin(scope, leftScope, leftRel, rightRel, condition, JoinRelType.INNER);
    }

    private RelNode convertLeftJoin(Scope scope, LeftJoin node) {
        RelNode leftRel  = convertTupleExpr(scope, node.getLeftArg());
        Scope leftScope =  Scopes.of(leftRel, scope); // FIXME: leftScope and rightScope must be local to condition and generate InputRef.
        RelNode rightRel = convertTupleExpr(leftScope,  node.getRightArg());

        List<String> columnNames = deriveNaturalJoinColumnList(leftRel.getRowType(), rightRel.getRowType());

        RexNode condition = convertUsing(leftRel.getRowType(), rightRel.getRowType(), columnNames);

        if (node.hasCondition()) {

            RexNode rightCond = convertExpression(Scopes.of(ImmutableList.of(leftRel, rightRel), scope), node.getCondition());
            // review: might contain inner queries (?)
            //rightRel = RelFactories.DEFAULT_FILTER_FACTORY.createFilter(rightRel, rightCond);
            condition = RelOptUtil.andJoinFilters(rexBuilder, condition, rightCond);
        }

        return createJoin(scope, leftScope, leftRel, rightRel, condition, JoinRelType.LEFT);
    }

    private RelNode createJoin(Scope scope,
                               Scope leftScope,
                               RelNode leftRel,
                               RelNode rightRel,
                               RexNode condition,
                               JoinRelType joinType) {

        CorrelateUse p = getCorrelateUse(leftScope, rightRel);

        if (p != null) {
            rightRel = p.r;

            RelNode corr =  RelFactories.DEFAULT_CORRELATE_FACTORY
                    .createCorrelate(leftRel,
                            rightRel,
                            p.id,
                            p.requiredColumns,
                            SemiJoinType.of(joinType));

            if (!condition.isAlwaysTrue())
                return RelFactories.DEFAULT_FILTER_FACTORY.createFilter(corr, condition);


            return corr;

        } else {

            org.apache.calcite.rel.core.Join r = (org.apache.calcite.rel.core.Join) RelFactories.DEFAULT_JOIN_FACTORY
                    .createJoin(leftRel,
                                rightRel,
                                condition,
                                ImmutableSet.of(),
                                joinType,
                                false);

            return RelOptUtil.pushDownJoinConditions(r);
        }
    }


    public static List<String> deriveNaturalJoinColumnList(
            RelDataType leftRowType,
            RelDataType rightRowType) {
        final List<String> naturalColumnNames = new ArrayList<>();
        final List<String> leftNames = leftRowType.getFieldNames();
        final List<String> rightNames = rightRowType.getFieldNames();
        for (String name : leftNames) {
            if ((Collections.frequency(leftNames, name) == 1)
                    && (Collections.frequency(rightNames, name) == 1)) {
                naturalColumnNames.add(name);
            }
        }
        return naturalColumnNames;
    }


    private RexNode convertUsing(RelDataType leftRowType, RelDataType rightRowType, List<String> nameList) {
        final List<RexNode> list = Lists.newArrayList();
        for (String name : nameList) {
            List<RexNode> operands = new ArrayList<>();
            int offset = 0;
            for (RelDataType rowType : ImmutableList.of(leftRowType,
                    rightRowType)) {
                final RelDataTypeField field = rowType.getField(name,false,false);
                operands.add(
                        rexBuilder.makeInputRef(field.getType(),
                                offset + field.getIndex()));
                offset += rowType.getFieldList().size();
            }
            list.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, operands));
        }
        // maybe use RelOptUtil.createEquiJoinCondition
        return RexUtil.composeConjunction(rexBuilder, list, false);
    }

    private RelNode convertLimit(Scope scope, Slice node) {
        BigDecimal fetch = BigDecimal.valueOf(node.getLimit());
        BigDecimal offset = BigDecimal.valueOf(node.getOffset());

        if (node.getArg() instanceof Order) {
            return convertOrder(scope, (Order)node.getArg(), offset, fetch);
        } else {
            RelNode input = convertTupleExpr(scope, node.getArg());
            return RelFactories.DEFAULT_SORT_FACTORY
                    .createSort(input, RelCollations.EMPTY,
                            rexBuilder.makeExactLiteral(offset),
                            rexBuilder.makeExactLiteral(fetch));
        }
    }

    private RelNode convertOrder(Scope scope, Order node, BigDecimal offset, BigDecimal fetch) {

        RelNode input = convertTupleExpr(scope, node.getArg());

        List<Pair<RexNode,String>> extensions = Lists.newArrayList();
        Map<OrderElem, String> mapElemToNames = Maps.newHashMap();

        List<String> columns = Lists.newArrayList(input.getRowType().getFieldNames());

        for (OrderElem elem : node.getElements()) {
            if (elem.getExpr() instanceof Var && !((Var)elem.getExpr()).isConstant()) {
                mapElemToNames.put(elem, ((Var)elem.getExpr()).getName());
            } else {
                String name = createUniqueColumnName(elem.getExpr(), columns);
                columns.add(name);
                mapElemToNames.put(elem, name);
                extensions.add(Pair.of(convertExpression(Scopes.of(input), elem.getExpr()), name));
            }
        }

        if (!extensions.isEmpty()) {
            ImmutableList.Builder<Pair<RexNode,String>> projections = ImmutableList.builder();

            RelDataType type = input.getRowType();

            for (RelDataTypeField f : type.getFieldList())
                projections.add(Pair.of(rexBuilder.makeInputRef(input,f.getIndex()), f.getName()));

            projections.addAll(extensions);

            input = RelOptUtil.createProject(input, projections.build(), false);
        }

        List<RelFieldCollation> fieldCollations = Lists.newArrayList();
        RelDataType type = input.getRowType();
        for (OrderElem elem : node.getElements()) {

            String fieldName = mapElemToNames.get(elem);
            assert fieldName != null;

            RelFieldCollation.Direction direction = (elem.isAscending()) ?
                    RelFieldCollation.Direction.ASCENDING : RelFieldCollation.Direction.DESCENDING;

            RelFieldCollation.NullDirection nullDirection = elem.isAscending() ?
                    RelFieldCollation.NullDirection.FIRST : RelFieldCollation.NullDirection.LAST;

            RelDataTypeField f = type.getField(fieldName, false, false);
            assert f != null;
            fieldCollations.add(new RelFieldCollation(f.getIndex(), direction, nullDirection));
        }

        RelCollation collation = RelCollations.of(fieldCollations);
        collation = cluster.traitSet().canonize(collation);


        input = RelFactories.DEFAULT_SORT_FACTORY
                .createSort(input, collation,
                        offset == null ? null : rexBuilder.makeExactLiteral(offset),
                        fetch == null ? null  : rexBuilder.makeExactLiteral(fetch));

        if (extensions.isEmpty())
            return input;
        else {
            List<RexNode> fields = Lists.newArrayList();
            for (String name : columns) {
                RelDataTypeField f = type.getField(name, false, false);
                fields.add(rexBuilder.makeInputRef(input, f.getIndex()));
            }

            return RelFactories.DEFAULT_PROJECT_FACTORY.createProject(input, fields, columns);
        }
    }

    protected String createUniqueColumnName(ValueExpr e, List<String> names) {
        String nameBase = "EXPR";
        for (int j = 0; ; j++) {
            String name = nameBase + j;
            if (!names.contains(name))
                return name;
        }
    }

    protected RelNode convertDistinct(Scope scope, Distinct node) {
        RelNode input = convertTupleExpr(scope, node.getArg());
        return RelOptUtil.createDistinctRel(input);
    }

    protected RelNode convertAgg(Scope scope, Group node) {
        RelNode input = convertTupleExpr(scope, node.getArg());

        List<String> nonGrouped = Lists.newArrayList(input.getRowType().getFieldNames());
        nonGrouped.removeAll(node.getGroupBindingNames());

        input = injectCorrelatedJoinConditions(scope, input, nonGrouped);

        ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();

        Scope localScope = Scopes.of(input);

        for (String groupName : node.getGroupBindingNames()) {
            Scope.ResolvedImpl result = new Scope.ResolvedImpl();
            localScope.resolve(groupName, result);
            assert result.resolved;
            int ordinal = result.field.getIndex();
            groupSet.set(ordinal);
        }

        List<AggregateCall> aggCalls = new ArrayList<>();

        for (GroupElem elem : node.getGroupElements()) {
            AggregateCall aggCall = convertAggCall(Scopes.of(input), input, elem.getOperator(), elem.getName());
            aggCalls.add(aggCall);
        }

        return RelFactories.DEFAULT_AGGREGATE_FACTORY
                .createAggregate(input,false, groupSet.build(),null, aggCalls);
    }

    private AggregateCall convertAggCall(Scope scope, RelNode input, AggregateOperator operator, String name) {
        boolean distinct = operator.isDistinct();

        List<Integer> argList = ImmutableList.of();

        ValueExpr arg = ((UnaryValueOperator)operator).getArg();

        // if arg is more complex expression than a new row-level calculation must be added a new column introduced
        if (arg instanceof Var) {
            if (!((Var) arg).isConstant()) {
                Scope.ResolvedImpl result = new Scope.ResolvedImpl();
                scope.resolve(((Var)arg).getName(), result);
                assert result.resolved && result.scope == scope;
                argList = ImmutableList.of(result.field.getIndex());
            }
        }

        ImmutableMap.Builder<Class<?>, SqlAggFunction> mapAggFuns = ImmutableMap.builder();

        mapAggFuns.put(Avg.class, SqlStdOperatorTable.AVG);
        mapAggFuns.put(Sum.class, SqlStdOperatorTable.SUM);
        mapAggFuns.put(Count.class, SqlStdOperatorTable.COUNT);
        mapAggFuns.put(Min.class, SqlStdOperatorTable.MIN);
        mapAggFuns.put(Max.class, SqlStdOperatorTable.MAX);
        mapAggFuns.put(Sample.class, SqlStdOperatorTable.COVAR_SAMP);
        //mapAggFuns.put(GroupConcat.class, SqlStdOperatorTable.)

        SqlAggFunction aggFun = mapAggFuns.build().get(operator.getClass());

        return AggregateCall.create(aggFun, distinct, argList, -1, 0,  input,null, name);
    }

    protected RelNode convertFilter(Scope scope, Filter node) {
        // FIXME: check whether this is a theta-join
        RelNode input = convertTupleExpr(scope, node.getArg());
        Scope expScope = Scopes.of(input, scope);
        RexNode cond = convertExpression(expScope, node.getCondition());

        RelNode filter = RelFactories.DEFAULT_FILTER_FACTORY.createFilter(input,cond);
        CorrelateUse p = getCorrelateUse(expScope, filter); // REVIEW scope
        if (p != null) {
            assert p.r instanceof org.apache.calcite.rel.core.Filter;
            org.apache.calcite.rel.core.Filter f = (org.apache.calcite.rel.core.Filter) p.r;
            filter = LogicalFilter.create(f.getInput(), f.getCondition(), ImmutableSet.of(p.id));
        }
        return filter;
    }

    protected RelNode convertProject(Scope scope, Projection node)
    {
        RelNode input = convertTupleExpr(scope, node.getArg());

        ImmutableList.Builder<Pair<RexNode,String>> projectionElems = ImmutableList.builder();

        List<String> nonProjected = Lists.newArrayList(input.getRowType().getFieldNames());

        for (ProjectionElem elem : node.getProjectionElemList().getElements())
            nonProjected.remove(elem.getSourceName());

        input = injectCorrelatedJoinConditions(scope, input, nonProjected);

        Scope localScope = Scopes.of(input);

        for (ProjectionElem elem : node.getProjectionElemList().getElements())  {

            RexNode exp;

            Scope.ResolvedImpl result = new Scope.ResolvedImpl();
            localScope.resolve(elem.getSourceName(),result);
            assert result.resolved;
            exp = rexBuilder.makeInputRef(result.rel, result.field.getIndex());

            projectionElems.add(Pair.of(exp, elem.getTargetName()));
        }

        return RelOptUtil.createProject(input, projectionElems.build(), false);
    }

    protected RelNode convertExtension(Scope scope, Extension node) {

        RelNode input = convertTupleExpr(scope, node.getArg());

        RelDataType rowType = input.getRowType();

        ImmutableList.Builder<Pair<RexNode,String>> extensionElems = ImmutableList.builder();

        extensionElems.addAll(Lists.<RelDataTypeField, Pair<RexNode,String>>transform(
                rowType.getFieldList(),
                f -> Pair.of(rexBuilder.makeInputRef(f.getType(), f.getIndex()),
                             f.getName())));

        boolean hasExtensions = false;

        for (ExtensionElem elem : node.getElements()) {
            if (!(elem.getExpr() instanceof AggregateOperator)) {
                RexNode exp = convertExpression(Scopes.of(input, scope), elem.getExpr());
                extensionElems.add(Pair.of(exp, elem.getName()));
                hasExtensions = true;
            }
        }
        if (hasExtensions)
            return RelOptUtil.createProject(input, extensionElems.build(), false);
        else
            return input;
    }

    protected RelNode convertBindingSetAssignment(Scope scope, BindingSetAssignment node) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> builder = ImmutableList.builder();

        ImmutableList<String> fieldNames = ImmutableList.copyOf(node.getBindingNames());

        RelDataTypeFactory.FieldInfoBuilder typeBuilder = cluster.getTypeFactory().builder();

        for (String name : fieldNames) {
            typeBuilder.add(name, SqlTypeName.ANY);
        }

        RelDataType rowType = typeBuilder.build();

        for (BindingSet bs : node.getBindingSets()) {
            ImmutableList<RexLiteral> tuple = ImmutableList.copyOf(fieldNames.stream()
                    .map(n -> makeLiteral(bs.getBinding(n).getValue())).iterator());
            builder.add(tuple);
        }
        return RelFactories.DEFAULT_VALUES_FACTORY.createValues(cluster, rowType, builder.build());
    }

    protected RexNode convertExpression(Scope scope, ValueExpr expr) {
        ValueExprConverter exprConverter = new ValueExprConverter(scope);
        return exprConverter.convertExpression(expr);
    }

    private RelNode injectCorrelatedJoinConditions(Scope scope, RelNode input, List<String> fieldNames) {

        final List<RexNode> list = Lists.newArrayList();

        RelDataType inputType = input.getRowType();

        for (String n : fieldNames) {
            Scope.ResolvedImpl result = new Scope.ResolvedImpl();
            scope.resolve(n, result);

            if (result.resolved) {

                List<RexNode> operands = Lists.newArrayList();

                CorrelationId id = cluster.createCorrel();
                RelDataType corrType = result.rel.getRowType();
                RexNode corrExp = rexBuilder.makeCorrel(corrType, id);

                DeferredLookup lookup = new DeferredLookup(id, result.scope, n, result.field.getIndex());
                mapCorrelToDeferred.put(id, lookup);

                operands.add(rexBuilder.makeFieldAccess(corrExp, result.field.getIndex()));

                RelDataTypeField field = inputType.getField(n, false, false);

                operands.add(
                        rexBuilder.makeInputRef(input, field.getIndex()));

                list.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, operands));
            }
        }

        RexNode condition = RexUtil.composeConjunction(rexBuilder, list, true);

        if (condition != null)
            input = RelFactories.DEFAULT_FILTER_FACTORY.createFilter(input, condition);

        return input;
    }

    protected RexLiteral makeLiteral(Value v) {
        if (v instanceof Literal) {
            return (RexLiteral) rexBuilder.makeRDFLiteral((Literal)v);
        }
        else
            return (RexLiteral) rexBuilder.makeRDFResource((Resource)v);
    }

    private CorrelateUse getCorrelateUse(Scope scope, RelNode node) {

        Set<CorrelationId> correlatedVariables = RelOptUtil.getVariablesUsed(node);

        if (correlatedVariables.isEmpty())
            return null;

        ImmutableBitSet.Builder columns = ImmutableBitSet.builder();

        List<CorrelationId> correlNames = Lists.newArrayList();

        for (CorrelationId id : correlatedVariables) {

            DeferredLookup lookup = mapCorrelToDeferred.get(id);

            if (lookup.scope.equals(scope)) {
                correlNames.add(id);
                columns.set(lookup.fieldOrd);
            }
        }

        if (correlNames.isEmpty())
            return null;

        RelNode r = node;

        if (correlNames.size() > 1) {
            r = DeduplicateCorrelateVariables.go(rexBuilder, correlNames.get(0), Util.skip(correlNames), r);
        }

        return new CorrelateUse(correlNames.get(0), columns.build(), r);
    }

    class ValueExprConverter extends AbstractQueryModelVisitor<RuntimeException>
            implements ValueExprToRexConverter.ValueExprConverterContext
    {
        private RexNode output;
        private Scope scope;

        public ValueExprConverter(Scope scope) {
            setScope(scope);
        }

        public void setScope(Scope scope) {
            this.scope = scope;
        }

        public RexNode convertExpression(ValueExpr exp) {
            ValueExprConverter converter = new ValueExprConverter(this.scope);
            exp.visit(converter);
            return converter.output;
        }

        public RexNode convertSubQuery(TupleExpr query) {
            RelNode rel = convertQueryRecursively(scope, query);
            return RexSubQuery.scalar(rel);
        }

        @Override public void meet(Var v) {

            if (v.isConstant()) {
                output = makeLiteral(v.getValue());
                return;
            }

            String varName = v.getName();

            /* resolve varName as local (i.e. column of the direct input) or global (i.e. set from
               a parent. If resolved failed, then something is wrong.
               If variable is local create an InputRef otherwise create a correlationId to the relation
               assosiated with the resolved variable, find its type and then create a FieldAccess to that correlation.
               useful for subqueries and leftjoin.
            */

            Scope.ResolvedImpl result = new Scope.ResolvedImpl();
            scope.resolve(varName, result);

            if (result.resolved) {

                if (result.scope == scope) {
                    // this is local
                    output = rexBuilder.makeInputRef(result.rel, result.field.getIndex());
                } else {
                    CorrelationId id = cluster.createCorrel();
                    RelDataType corrType = result.rel.getRowType();
                    RexNode corrExp = rexBuilder.makeCorrel(corrType, id);
                    DeferredLookup lookup = new DeferredLookup(id, result.scope, varName, result.field.getIndex());
                    mapCorrelToDeferred.put(id, lookup);
                    output = rexBuilder.makeFieldAccess(corrExp, result.field.getIndex());
                }
            }
        }

        @Override public void meet(ValueConstant node) {
            output = makeLiteral(node.getValue());
        }

        @Override public void meet(In node) {
            RexNode arg = convertExpression(node.getArg());
            RelNode rel = convertQueryRecursively(scope, node.getSubQuery());
            output = RexSubQuery.in(rel, ImmutableList.of(arg));
        }

        @Override public void meet(Exists node) {
            RelNode rel = convertQueryRecursively(scope, node.getSubQuery());
            output = RexSubQuery.exists(rel);
        }

        @Override protected void meetNode(QueryModelNode node) {
            if (node instanceof ValueExpr)
                output = exprConverter.convertExpr((ValueExpr)node, this);
            else
                super.meetNode(node);
        }
    }

    interface Scope {

        void resolve(String name, Resolved result);

        Scope getParent();

        interface Resolved {

            void success(Scope scope, RelNode rel, RelDataTypeField field);
            void failed();
        }

        class ResolvedImpl implements Resolved {

            boolean resolved;
            Scope scope;
            RelNode rel;
            RelDataTypeField field;

            public void success(Scope scope, RelNode rel, RelDataTypeField field){
                this.resolved = true;
                this.scope = scope;
                this.rel = rel;
                this.field = field;
            }

            public void failed() {
                resolved = false;
            }
        }
    }

    public static class EmptyScope implements Scope {

        public void resolve(String name, Resolved result) {
            result.failed();
        }

        public Scope getParent() { return null; }
    }

    public static class DelegatingScope implements Scope {

        private Scope parentScope;

        public DelegatingScope(Scope delegate) {
            parentScope = delegate;
        }

        public void resolve(String name, Resolved result) {
            if (parentScope != null) {
                parentScope.resolve(name, result);
            } else {
                result.failed();
            }

        }
        public Scope getParent() { return parentScope; }

    }

    public static class RelNodeScope extends  DelegatingScope {

        private ImmutableList<RelNode> input;

        public RelNodeScope(Scope delegate, List<RelNode> input) {
            super(delegate);
            this.input = ImmutableList.copyOf(input);
        }

        public void resolve(String name, Resolved result) {

            for (RelNode input : this.input) {
                RelDataType type = input.getRowType();
                RelDataTypeField field = type.getField(name, false, false);
                if (field != null) {
                    result.success(this, input, field);
                    return;
                }
            }
            super.resolve(name, result);
        }
    }

    public static class Scopes {

        static Scope empty() { return new EmptyScope(); }

        static Scope of(RelNode rel) {
            return new RelNodeScope(null, ImmutableList.of(rel));
        }

        static Scope of(List<RelNode> rel) { return new RelNodeScope(null, rel); }

        static Scope of(RelNode rel, Scope parent) {
            return new RelNodeScope(parent, ImmutableList.of(rel));
        }

        static Scope of(List<RelNode> rel, Scope parent) { return new RelNodeScope(parent, rel); }

        static Scope delegate(Scope parent) { return new RelNodeScope(parent, ImmutableList.of()); }

    }

    static class CorrelateUse {

        CorrelationId id;
        ImmutableBitSet requiredColumns;
        RelNode r;

        CorrelateUse(CorrelationId id, ImmutableBitSet columns, RelNode r) {
            this.id = id;
            this.requiredColumns = columns;
            this.r = r;
        }
    }

    static class DeferredLookup {
        CorrelationId id;
        Scope scope;
        String originalFieldName;
        int fieldOrd;

        DeferredLookup(CorrelationId id, Scope scope, String originalFieldName, int fieldOrd) {
            this.id = id;
            this.scope = scope;
            this.originalFieldName = originalFieldName;
            this.fieldOrd = fieldOrd;
        }
    }

}