package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql.ConvexHull;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StConvexHull implements Function {
    
	@Override
    public String getURI() {
        return STRDF.convexHull.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        return new ConvexHull().evaluate(valueFactory, values);
    }
    
}