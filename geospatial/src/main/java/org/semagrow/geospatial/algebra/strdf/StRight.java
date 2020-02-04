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

public class StRight implements Function {
	
    @Override
    public String getURI() {
        return STRDF.right.toString();
    }
    
    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
    	if (values.length != 2) {
			throw new ValueExprEvaluationException(getURI() + " requires exactly 2 arguments, got " + values.length);
		}

        Value value1 = new StMbb().evaluate(valueFactory, values[0]);
        Value value2 = new StMbb().evaluate(valueFactory, values[1]);
        
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
        
        boolean right = SimpleGeometryCoordinates.minX(mbb1) > SimpleGeometryCoordinates.maxX(mbb2);
        Value value = valueFactory.createLiteral(right);
        return value;
    }
}