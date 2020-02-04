package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.locationtech.jts.geom.Geometry;
import org.semagrow.geospatial.commons.IllegalGeometryException;
import org.semagrow.geospatial.commons.SimpleGeometryCoordinates;
import org.semagrow.geospatial.commons.SimpleGeometryFactory;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StAbove implements Function {
	
    @Override
    public String getURI() {
        return STRDF.above.toString();
    }
    
    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
    	if (values.length != 2) {
			throw new ValueExprEvaluationException(getURI() + " requires exactly 2 arguments, got " + values.length);
		}

        Value value1 = new StEnvelope().evaluate(valueFactory, values[0]);
        Value value2 = new StEnvelope().evaluate(valueFactory, values[1]);
        
        String wktString1 = value1.stringValue();
        String wktString2 = value2.stringValue();
        Geometry mbb1 = null, mbb2 = null;
        try {
        	mbb1 = SimpleGeometryFactory.getInstance().createGeometry(wktString1);
        	mbb2 = SimpleGeometryFactory.getInstance().createGeometry(wktString2);
        } catch (IllegalGeometryException e) {
            throw new ValueExprEvaluationException("Illegal WKT format", e);
        } catch (RuntimeException e) {
			throw new ValueExprEvaluationException(e);
		}
        
        boolean above = SimpleGeometryCoordinates.minY(mbb1) > SimpleGeometryCoordinates.maxY(mbb2);
        Value value = valueFactory.createLiteral(above);
        return value;
    }
}