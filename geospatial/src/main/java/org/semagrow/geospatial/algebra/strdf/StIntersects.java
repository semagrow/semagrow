package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql.SfIntersects;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StIntersects implements Function {
	
    @Override
    public String getURI() {
        return STRDF.intersects.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        return new SfIntersects().evaluate(valueFactory, values);
    }
    
}