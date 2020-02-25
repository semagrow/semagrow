package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql.Buffer;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StBuffer implements Function {
    
	@Override
    public String getURI() {
        return STRDF.buffer.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        return new Buffer().evaluate(valueFactory, values);
    }
    
}