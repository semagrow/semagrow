package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.semagrow.geospatial.commons.SimpleGeometryCoordinates;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StMbbContains implements Function {
	
    @Override
    public String getURI() {
        return STRDF.mbbContains.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
    	if (values.length != 2) {
			throw new ValueExprEvaluationException(getURI() + " requires exactly 2 arguments, got " + values.length);
		}

        Value value1 = new StEnvelope().evaluate(valueFactory, values[0]);
        Value value2 = new StEnvelope().evaluate(valueFactory, values[1]);
        return new StContains().evaluate(valueFactory, value1, value2);
    }
}
