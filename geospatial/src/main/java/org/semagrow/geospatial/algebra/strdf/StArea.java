package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.semagrow.geospatial.commons.IllegalGeometryException;
import org.semagrow.geospatial.commons.SimpleShapeFactory;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StArea implements Function {
    @Override
    public String getURI() {
        return STRDF.area.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        if (values.length != 1) {
            throw new ValueExprEvaluationException("Wrong number of arguments");
        }
        if (!(values[0] instanceof Literal && ((Literal) values[0]).getDatatype().equals(GEO.WKT_LITERAL))) {
            throw new ValueExprEvaluationException("Illegal argument format");
        }

        String wktString = values[0].stringValue();
        Shape shape = null;
        try {
            shape = SimpleShapeFactory.getInstance().createShape(wktString);
        } catch (IllegalGeometryException e) {
            throw new ValueExprEvaluationException("Illegal WKT format", e);
        }
        double area = shape.getArea(SpatialContext.GEO);
        Value value = valueFactory.createLiteral(area);
        return  value;
    }
}
