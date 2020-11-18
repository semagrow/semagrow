package org.semagrow.geospatial.selector;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEOF;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.semagrow.geospatial.helpers.WktHelpers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class BBoxSourcePruner {

    private static final Set<String> nonDisjointStRelations;
    private static final Set<Compare.CompareOp> leqOperators;

    private static final Literal TRUE;
    private static final Literal FALSE;

    private static final EvaluationStrategy strategy;

    static {
        final ValueFactory vf = SimpleValueFactory.getInstance();

        TRUE = vf.createLiteral(true);
        FALSE = vf.createLiteral(false);

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

        leqOperators = new HashSet<>();

        leqOperators.add(Compare.CompareOp.LE);
        leqOperators.add(Compare.CompareOp.LT);
        leqOperators.add(Compare.CompareOp.EQ);

        strategy = new StrictEvaluationStrategy(new TripleSource() {
            @Override
            public CloseableIteration<? extends Statement, QueryEvaluationException>
                        getStatements( Resource resource,
                                       IRI iri,
                                       Value value,
                                       Resource... resources) throws QueryEvaluationException {
                return null;
            }

            @Override
            public ValueFactory getValueFactory() {
                return vf;
            }
        }, s -> null);
    }

    public static boolean emptyResultSet(ValueExpr filter, String varName, Literal boundingBox) {
        Map<String, Literal> var_bbox = new HashMap<>();
        var_bbox.put(varName, boundingBox);
        ValueExpr expr = rewriteFilter(filter, var_bbox);
        Value value = strategy.evaluate(expr, new EmptyBindingSet());
        return value.equals(TRUE);
    }

    public static boolean emptyResultSet(ValueExpr filter, String varName1, Literal boundingBox1, String varName2, Literal boundingBox2) {
        Map<String, Literal> var_bbox = new HashMap<>();
        var_bbox.put(varName1, boundingBox1);
        var_bbox.put(varName2, boundingBox2);
        ValueExpr expr = rewriteFilter(filter, var_bbox);
        Value value = strategy.evaluate(expr, new EmptyBindingSet());
        return value.equals(TRUE);
    }

    private static ValueExpr rewriteFilter(ValueExpr valueExpr, Map<String, Literal> var_bbox) {

        if (valueExpr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) valueExpr;

            if (nonDisjointStRelations.contains(func.getURI())) {
                try {
                    FunctionCall new_func = new FunctionCall();
                    new_func.setURI(GEOF.SF_DISJOINT.stringValue());
                    ValueConstant arg0 = getValue(func.getArgs().get(0), var_bbox);
                    ValueConstant arg1 = getValue(func.getArgs().get(1), var_bbox);
                    new_func.addArgs(arg0, arg1);
                    return new_func;
                } catch (UnboundVariableException e) {
                    return new ValueConstant(FALSE);
                }
            }

        }
        if (valueExpr instanceof Compare) {
            Compare compare = (Compare) valueExpr;

            if (leqOperators.contains(compare.getOperator())) {
                FunctionCall func = (FunctionCall) compare.getLeftArg();

                if (func.getURI().equals(GEOF.DISTANCE.stringValue()) ) {
                    try {
                        FunctionCall buffer = new FunctionCall();
                        buffer.setURI(GEOF.BUFFER.stringValue());
                        ValueConstant buffarg = getValue(func.getArgs().get(1), var_bbox);
                        buffer.addArgs(buffarg, compare.getRightArg(), func.getArgs().get(2));

                        FunctionCall new_func = new FunctionCall();
                        new_func.setURI(GEOF.SF_DISJOINT.stringValue());
                        ValueConstant disjarg = getValue(func.getArgs().get(0), var_bbox);
                        new_func.addArgs(disjarg, buffer);

                        return new_func;
                    } catch (UnboundVariableException e) {
                        return new ValueConstant(FALSE);
                    }
                }
            }
        }
        return new ValueConstant(FALSE);
    }

    private static ValueConstant getValue(ValueExpr arg, Map<String, Literal> var_bbox) throws UnboundVariableException {
        if (arg instanceof ValueConstant) {
            return (ValueConstant) arg;
        }
        if (arg instanceof Var && var_bbox.containsKey(((Var) arg).getName())) {
            Literal bbox = var_bbox.get(((Var) arg).getName());
            bbox = WktHelpers.removeCRSfromWKT(bbox);
            return new ValueConstant(bbox);
        }
        throw new UnboundVariableException();
    }

    private static class UnboundVariableException extends Exception {
        public UnboundVariableException() {
            super();
        }
    }
}
