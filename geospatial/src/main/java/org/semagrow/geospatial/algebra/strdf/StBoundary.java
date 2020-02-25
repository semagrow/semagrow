package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql.Boundary;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StBoundary implements Function {
    
	@Override
    public String getURI() {
        return STRDF.boundary.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        return new Boundary().evaluate(valueFactory, values);
    }
    
}