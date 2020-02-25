package org.semagrow.geospatial.algebra.strdf;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql.SfContains;
import org.semagrow.geospatial.vocabulary.STRDF;

public class StContains implements Function {

	@Override
	public String getURI() {
		return STRDF.contains.toString();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
		return new SfContains().evaluate(valueFactory, values);
	}

}
