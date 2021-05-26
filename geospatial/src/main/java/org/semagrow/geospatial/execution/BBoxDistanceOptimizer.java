package org.semagrow.geospatial.execution;

import com.esri.core.geometry.*;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.GEOF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.geospatial.vocabulary.UOM;

import java.util.HashSet;
import java.util.Set;

import static com.esri.core.geometry.WktExportFlags.wktExportDefaults;
import static com.esri.core.geometry.WktImportFlags.wktImportDefaults;

public class BBoxDistanceOptimizer implements QueryOptimizer {

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet) {
        tupleExpr.visit(new DistanceFilterFinder(tupleExpr, bindingSet));
    }

    protected static class DistanceFilterFinder extends AbstractQueryModelVisitor<RuntimeException> {

        protected final TupleExpr tupleExpr;
        protected final BindingSet bindingSet;
        private static final Set<Compare.CompareOp> leqOperators;

        static {
            leqOperators = new HashSet<>();
            leqOperators.add(Compare.CompareOp.LE);
            leqOperators.add(Compare.CompareOp.LT);
            leqOperators.add(Compare.CompareOp.EQ);
        }

        public DistanceFilterFinder(TupleExpr tupleExpr, BindingSet bindingSet) {
            this.tupleExpr = tupleExpr;
            this.bindingSet = bindingSet;
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

                        double distance = Double.parseDouble(distanceString);

                        if (arg3 instanceof ValueConstant) {
                            Value unit = ((ValueConstant) arg3).getValue();

                            if (unit.equals(UOM.metre)) {
                                distance = approxMetersToDegrees(distance);
                            }

                            if (getArgValue(arg1, bindingSet) == null && getArgValue(arg2, bindingSet) != null) {
                                assert (arg1 instanceof Var);
                                expand(filter, (Var) arg1, getArgValue(arg2, bindingSet), distance);
                            }

                            if (getArgValue(arg2, bindingSet) == null && getArgValue(arg1, bindingSet) != null) {
                                assert (arg2 instanceof Var);
                                expand(filter, (Var) arg2, getArgValue(arg1, bindingSet), distance);
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

        private void expand(Filter filter, Var arg, String knownWKT, double distance) {

            FunctionCall func = new FunctionCall();
            func.setURI(GEOF.SF_INTERSECTS.stringValue());
            func.addArgs(arg, new ValueConstant((BufferedBBox(knownWKT,distance))));

            Filter expansion = new Filter();
            expansion.setCondition(func);
            expansion.setArg(filter.getArg());
            filter.setArg(expansion);
        }

        private Value BufferedBBox(String knownWKT, double distance) {
            ValueFactory vf = SimpleValueFactory.getInstance();

            /* http://esri.github.io/geometry-api-java/doc/Buffer.html */
            Geometry g = OperatorImportFromWkt.local().execute(wktImportDefaults, Geometry.Type.Unknown, knownWKT, null);
            SpatialReference spatialRef = SpatialReference.create(4326);
            Geometry b = OperatorBuffer.local().execute(g, spatialRef, distance, null);
            Envelope e = new Envelope();
            b.queryEnvelope(e);
            String str = OperatorExportToWkt.local().execute(wktExportDefaults, e, null);
            Value value = vf.createLiteral(str, GEO.WKT_LITERAL);

            return value;
        }
    }

    private static double approxMetersToDegrees(double distance) {
        /* this calculation is an over-estimation, because at this stage we simply want to filter non relevant geometries */
        distance = 1.3 * distance;
        return distance * 0.00001 / 1.1132;
        /* 0.00001 degrees equal 1.1132 meters at equator */
    }

}
