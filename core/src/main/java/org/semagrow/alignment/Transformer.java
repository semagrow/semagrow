package org.semagrow.alignment;

import org.eclipse.rdf4j.model.IRI;

/**
 * Created by angel on 12/2/14.
 */
public interface Transformer<A,B> {

    int getId();

    IRI getSourceSchema();

    IRI getTargetSchema();

    double getProximity();

    B transform(A source);

    A transformBack(B target);
}
