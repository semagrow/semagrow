package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.locationtech.jts.geom.Geometry;
import org.semagrow.geospatial.commons.IllegalGeometryException;
import org.semagrow.geospatial.commons.SimpleGeometryFactory;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StMbb implements Function {
	
    @Override
    public String getURI() {
        return STRDF.mbbIntersects.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
    	if (values.length != 1) {
			throw new ValueExprEvaluationException(getURI() + " requires exactly 1 argument, got " + values.length);
		}
        if (!(values[0] instanceof Literal && ((Literal) values[0]).getDatatype().equals(GEO.WKT_LITERAL))) {
            throw new ValueExprEvaluationException("Illegal argument format");
        }

        String wktString = values[0].stringValue();
        Geometry geometry = null;
        try {
        	geometry = SimpleGeometryFactory.getInstance().createGeometry(wktString);
        } catch (IllegalGeometryException e) {
            throw new ValueExprEvaluationException("Illegal WKT format", e);
        } catch (RuntimeException e) {
			throw new ValueExprEvaluationException(e);
		}
        Geometry mbb = geometry.getEnvelope();
        wktString = mbb.toText();
        Value value = valueFactory.createLiteral(wktString);
        return value;
    }
}
