package org.semagrow.geospatial.selector;

import com.esri.core.geometry.*;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEOF;
import org.eclipse.rdf4j.query.algebra.*;
import org.semagrow.geospatial.helpers.WktHelpers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.esri.core.geometry.WktImportFlags.wktImportDefaults;

public final class BBoxSourcePruner {

    private static final Set<String> nonDisjointStRelations;
    private static final Set<String> nonTouchingStRelations;
    private static final Set<Compare.CompareOp> leqOperators;

    static {
        final ValueFactory vf = SimpleValueFactory.getInstance();

        nonDisjointStRelations = new HashSet<>();

        nonDisjointStRelations.add(GEOF.SF_EQUALS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_INTERSECTS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_TOUCHES.stringValue());
        nonDisjointStRelations.add(GEOF.SF_WITHIN.stringValue());
        nonDisjointStRelations.add(GEOF.SF_CONTAINS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_OVERLAPS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_CROSSES.stringValue());

        nonDisjointStRelations.add(GEOF.EH_EQUALS.stringValue());
        nonDisjointStRelations.add(GEOF.EH_MEET.stringValue());
        nonDisjointStRelations.add(GEOF.EH_OVERLAP.stringValue());
        nonDisjointStRelations.add(GEOF.EH_COVERS.stringValue());
        nonDisjointStRelations.add(GEOF.EH_COVERED_BY.stringValue());
        nonDisjointStRelations.add(GEOF.EH_INSIDE.stringValue());
        nonDisjointStRelations.add(GEOF.EH_CONTAINS.stringValue());

        nonDisjointStRelations.add(GEOF.RCC8_EQ.stringValue());
        nonDisjointStRelations.add(GEOF.RCC8_EC.stringValue());
        nonDisjointStRelations.add(GEOF.RCC8_PO.stringValue());
        nonDisjointStRelations.add(GEOF.RCC8_TPP.stringValue());
        nonDisjointStRelations.add(GEOF.RCC8_NTPPI.stringValue());
        nonDisjointStRelations.add(GEOF.RCC8_NTPP.stringValue());
        nonDisjointStRelations.add(GEOF.RCC8_TPPI.stringValue());

        nonTouchingStRelations = new HashSet<>();

        nonTouchingStRelations.add(GEOF.SF_EQUALS.stringValue());
        nonTouchingStRelations.add(GEOF.SF_WITHIN.stringValue());
        nonTouchingStRelations.add(GEOF.SF_CONTAINS.stringValue());

        nonDisjointStRelations.add(GEOF.EH_EQUALS.stringValue());
        nonDisjointStRelations.add(GEOF.EH_INSIDE.stringValue());
        nonDisjointStRelations.add(GEOF.EH_CONTAINS.stringValue());

        nonDisjointStRelations.add(GEOF.RCC8_EQ.stringValue());

        leqOperators = new HashSet<>();

        leqOperators.add(Compare.CompareOp.LE);
        leqOperators.add(Compare.CompareOp.LT);
        leqOperators.add(Compare.CompareOp.EQ);
    }

    public static boolean isGeospatialFilter(ValueExpr valueExpr) {

        if (valueExpr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) valueExpr;
            return nonDisjointStRelations.contains(func.getURI());
        }

        if (valueExpr instanceof Compare) {
            Compare compare = (Compare) valueExpr;
            if (leqOperators.contains(compare.getOperator()) && compare.getLeftArg() instanceof FunctionCall) {
                FunctionCall func = (FunctionCall) compare.getLeftArg();
                return func.getURI().equals(GEOF.DISTANCE.stringValue());
            }
            return false;
        }

        return false;
    }

    public static boolean emptyResultSet(ValueExpr filter, String varName, Literal boundingBox) {
        Map<String, Literal> var_bbox = new HashMap<>();
        var_bbox.put(varName, boundingBox);
        return emptyResultSetInternal(filter, var_bbox);
    }

    public static boolean emptyResultSet(ValueExpr filter, String varName1, Literal boundingBox1, String varName2, Literal boundingBox2) {
        Map<String, Literal> var_bbox = new HashMap<>();
        var_bbox.put(varName1, boundingBox1);
        var_bbox.put(varName2, boundingBox2);
        return emptyResultSetInternal(filter, var_bbox);
    }

    private static boolean emptyResultSetInternal(ValueExpr valueExpr, Map<String, Literal> var_bbox) {

        SpatialReference sr = SpatialReference.create(4326);

        try {
            if (valueExpr instanceof FunctionCall) {
                FunctionCall func = (FunctionCall) valueExpr;

                if (nonDisjointStRelations.contains(func.getURI())) {
                    Geometry arg1 = getGeometry(func.getArgs().get(0), var_bbox);
                    Geometry arg2 = getGeometry(func.getArgs().get(1), var_bbox);

                    if (OperatorDisjoint.local().execute(arg1, arg2, sr, null)) {
                        return true;
                    }
                }

                if (nonTouchingStRelations.contains(func.getURI())) {
                    Geometry arg1 = getGeometry(func.getArgs().get(0), var_bbox);
                    Geometry arg2 = getGeometry(func.getArgs().get(1), var_bbox);

                    if (OperatorTouches.local().execute(arg1, arg2, sr, null)) {
                        return true;
                    }
                }
            }
            if (valueExpr instanceof Compare) {
                Compare compare = (Compare) valueExpr;

                if (leqOperators.contains(compare.getOperator())) {
                    FunctionCall func = (FunctionCall) compare.getLeftArg();
                    String distanceString = ((ValueConstant) compare.getRightArg()).getValue().stringValue();
                    double distance = Double.parseDouble(distanceString);

                    if (func.getURI().equals(GEOF.DISTANCE.stringValue()) ) {
                        Geometry arg = getGeometry(func.getArgs().get(0), var_bbox);
                        Geometry buf = getGeometry(func.getArgs().get(1), var_bbox);
                        ValueExpr arg3 = func.getArgs().get(2);

                        if (arg3 instanceof ValueConstant) {
                            Value unit = ((ValueConstant) arg3).getValue();

                            if (unit.stringValue().equals("http://www.opengis.net/def/uom/OGC/1.0/metre")) { //FIXME
                                distance = approxMetersToDegrees(distance);
                            }

                            Geometry buffer = OperatorBuffer.local().execute(buf, sr, distance, null);

                            if (OperatorDisjoint.local().execute(arg, buffer, sr, null)) {
                                return true;
                            }
                        }
                    }
                }
            }
        } catch (UnboundVariableException e) {
            return false;
        }
        return false;
    }

    private static Geometry getGeometry(ValueExpr arg, Map<String, Literal> var_bbox) throws UnboundVariableException {
        if (arg instanceof ValueConstant) {
            String wkt = ((ValueConstant) arg).getValue().stringValue();
            Geometry g = OperatorImportFromWkt.local().execute(wktImportDefaults, Geometry.Type.Unknown, wkt, null);
            return g;
        }
        if (arg instanceof Var && var_bbox.containsKey(((Var) arg).getName())) {
            Literal bbox = var_bbox.get(((Var) arg).getName());
            bbox = WktHelpers.removeCRSfromWKT(bbox);
            String wkt = bbox.stringValue();
            Geometry g = OperatorImportFromWkt.local().execute(wktImportDefaults, Geometry.Type.Unknown, wkt, null);
            return g;
        }
        throw new UnboundVariableException();
    }

    private static double approxMetersToDegrees(double distance) {
        /* this calculation is an over-estimation */
        return distance * 0.00001 / 1.1132;
        /* 0.00001 degrees equal 1.1132 meters at equator */
    }

    private static class UnboundVariableException extends Exception {
        public UnboundVariableException() {
            super();
        }
    }
}
