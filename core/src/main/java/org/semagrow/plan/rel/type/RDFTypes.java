package org.semagrow.plan.rel.type;

import org.apache.calcite.rel.type.RelDataType;

/**
 * Created by angel on 7/7/2017.
 */
public class RDFTypes {

    static public RelDataType PLAIN_LITERAL = new BasicRdfType("RDF PLAIN LITERAL");

    static public RelDataType IRI_TYPE   = new BasicRdfType("RDF IRI");
    static public RelDataType BLANK_NODE = new BasicRdfType("RDF BLANK NODE");

}
