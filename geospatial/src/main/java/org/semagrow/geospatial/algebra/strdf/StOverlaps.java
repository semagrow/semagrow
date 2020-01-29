package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql.SfOverlaps;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StOverlaps implements Function {
	
    @Override
    public String getURI() {
        return STRDF.overlaps.toString();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
        return new SfOverlaps().evaluate(valueFactory, values);
    }
    
}