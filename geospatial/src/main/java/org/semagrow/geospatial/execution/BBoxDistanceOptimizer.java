package org.semagrow.geospatial.execution;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.GEOF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.util.SimpleBindingSetOps;

import java.util.*;

public class BBoxDistanceOptimizer implements QueryOptimizer {

    /* these variables are to be initialized after calling optimize() */

    private Var freeVar;
    private Var boundVar;
    private Var buffbboxVar;
    private double distance;
    private IRI uom;

    private BindingSetOps bindingSetOps = SimpleBindingSetOps.getInstance();

    /*
     * finds the filter of the form: FILTER( distance(?free, ?bound, U) < D ), where ?bound is included in the bindingSet
     * and then adds FILTER( sfIntersects(?free, ?buffbbox) ), so that ?buffbbox to contain the buffered bbox
     */

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet) {
        tupleExpr.visit(new DistanceFilterFinder(tupleExpr, bindingSet));
    }

    /*
     * for each binding of ?bound, we need to expand the bindings b with ?buffbbox such that
     * where b(?buffbbox) = a buffered bbox with size D around b(?bound)
     */

    public BindingSet expandBindings(BindingSet bindingSet) {

        String wkt = bindingSet.getBinding(boundVar.getName()).getValue().stringValue();
        Value bbox = BBoxBuilder.getBufferedBBox(wkt, distance, uom);
        QueryBindingSet b = new QueryBindingSet();
        b.addBinding(buffbboxVar.getName(), bbox);

        return bindingSetOps.merge(bindingSet, b);
    }

    public List<BindingSet> expandBindings(List<BindingSet> bindingSetList) {
        List<BindingSet> list = new ArrayList<>();
        for (BindingSet b: bindingSetList) {
            list.add(expandBindings(b));
        }
        return list;
    }

    private class DistanceFilterFinder extends AbstractQueryModelVisitor<RuntimeException> {

        protected final TupleExpr tupleExpr;
        protected final BindingSet bindingSet;
        private final Set<Compare.CompareOp> leqOperators;

        public DistanceFilterFinder(TupleExpr tupleExpr, BindingSet bindingSet) {
            this.tupleExpr = tupleExpr;
            this.bindingSet = bindingSet;

            this.leqOperators = new HashSet<>();
            this.leqOperators.add(Compare.CompareOp.LE);
            this.leqOperators.add(Compare.CompareOp.LT);
            this.leqOperators.add(Compare.CompareOp.EQ);
        }

        public void meet(Filter filter) {
            ValueExpr condition = filter.getCondition();

            if (condition instanceof Compare && ((Compare) condition).getLeftArg() instanceof FunctionCall) {
                Compare compare = (Compare) condition;

                if (leqOperators.contains(compare.getOperator())) {
                    FunctionCall func = (FunctionCall) compare.getLeftArg();
                    String distanceString = getArgValue(compare.getRightArg(), bindingSet);

                    if (func.getURI().equals(GEOF.DISTANCE.stringValue()) && distanceString != null) {
                        ValueExpr arg1 = func.getArgs().get(0);
                        ValueExpr arg2 = func.getArgs().get(1);
                        ValueExpr arg3 = func.getArgs().get(2);

                        distance = Double.parseDouble(distanceString);

                        if (arg3 instanceof ValueConstant) {
                            Value uomValue = ((ValueConstant) arg3).getValue();

                            if (uomValue instanceof IRI && arg1 instanceof Var && arg2 instanceof Var) {

                                String arg1name = ((Var) arg1).getName();
                                String arg2name = ((Var) arg2).getName();
                                uom = (IRI) uomValue;

                                if (!(bindingSet.hasBinding(arg1name)) && bindingSet.hasBinding(arg2name)) {
                                    freeVar = (Var) arg1;
                                    boundVar = (Var) arg2;
                                    buffbboxVar = initializeBufferedBBoxVar(arg1name);
                                    expand(filter);
                                }

                                if (!(bindingSet.hasBinding(arg2name)) && bindingSet.hasBinding(arg1name)) {
                                    freeVar = (Var) arg2;
                                    boundVar = (Var) arg1;
                                    buffbboxVar = initializeBufferedBBoxVar(arg2name);
                                    expand(filter);
                                }
                            }
                        }
                    }
                }
            }
            filter.getArg().visit(this);
        }

        private String getArgValue(ValueExpr arg, BindingSet b) {
            if (arg instanceof ValueConstant) {
                return ((ValueConstant) arg).getValue().stringValue();
            }
            else {
                if (arg instanceof Var && b.hasBinding(((Var) arg).getName())) {
                    return b.getBinding(((Var) arg).getName()).getValue().stringValue();
                }
            }
            return null;
        }

        private Var initializeBufferedBBoxVar(String var) {
            String varname = "BBBox_of_" + var + "_" + UUID.randomUUID().toString().substring(0,7);
            return new Var(varname);
        }

        private void expand(Filter filter) {
            FunctionCall func = new FunctionCall();
            func.setURI(GEOF.SF_INTERSECTS.stringValue());
            func.addArgs(freeVar, buffbboxVar);

            Filter expansion = new Filter();
            expansion.setCondition(func);
            expansion.setArg(filter.getArg());
            filter.setArg(expansion);
        }
    }
}
