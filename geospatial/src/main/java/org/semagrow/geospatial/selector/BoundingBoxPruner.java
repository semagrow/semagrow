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

import java.util.HashSet;
import java.util.Set;

public final class BoundingBoxPruner {

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

    public static boolean prune(ValueExpr filter, String varName, Literal boundingBox) {
        ValueExpr expr = rewriteFilter(filter, varName, boundingBox);
        Value value = strategy.evaluate(expr, new EmptyBindingSet());
        return value.equals(TRUE);
    }

    private static ValueExpr rewriteFilter(ValueExpr valueExpr, String varName, Value boundingBox) {

        Value bboxWKT = WktHelpers.removeCRSfromWKT((Literal) boundingBox);

        if (valueExpr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) valueExpr;

            if (checkArgs(func, varName)) {
                if (nonDisjointStRelations.contains(func.getURI())) {

                    FunctionCall new_func = new FunctionCall();
                    new_func.setURI(GEOF.SF_DISJOINT.stringValue());
                    new_func.addArgs(new ValueConstant(bboxWKT), func.getArgs().get(1));

                    return new_func;
                }
            }
        }
        if (valueExpr instanceof Compare) {
            Compare compare = (Compare) valueExpr;

            if (leqOperators.contains(compare.getOperator())) {
                FunctionCall func = (FunctionCall) compare.getLeftArg();

                if (func.getURI().equals(GEOF.DISTANCE.stringValue()) && checkArgs(func, varName)) {

                    FunctionCall buffer = new FunctionCall();
                    buffer.setURI(GEOF.BUFFER.stringValue());
                    buffer.addArgs(func.getArgs().get(1), compare.getRightArg(), func.getArgs().get(2));
                    new ValueConstant(bboxWKT);

                    FunctionCall new_func = new FunctionCall();
                    new_func.setURI(GEOF.SF_DISJOINT.stringValue());
                    new_func.addArgs(new ValueConstant(bboxWKT), buffer);

                    return new_func;
                }
            }
        }
        return new ValueConstant(FALSE);
    }

    private static boolean checkArgs(FunctionCall functionCall, String varName) {
        ValueExpr arg0 = functionCall.getArgs().get(0);
        return (arg0 instanceof Var && ((Var) arg0).getName().equals(varName));
    }
}
