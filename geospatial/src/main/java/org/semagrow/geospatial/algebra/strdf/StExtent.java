package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StExtent implements Function {
    
	@Override
    public String getURI() {
        return STRDF.extent.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        Value sets = new StUnion().evaluate(valueFactory, values);
        Value ret = new StEnvelope().evaluate(valueFactory, sets);
    	return ret;
    }
    
}