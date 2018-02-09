package org.semagrow.plan.rel.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.eclipse.rdf4j.model.IRI;


public interface RdfDataTypeFactory extends RelDataTypeFactory {

    RelDataType createRdfDataType(IRI datatypeIRI);

}
