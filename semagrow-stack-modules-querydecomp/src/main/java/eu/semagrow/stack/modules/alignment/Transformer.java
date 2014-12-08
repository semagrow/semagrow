package eu.semagrow.stack.modules.alignment;

import org.openrdf.model.URI;

/**
 * Created by angel on 12/2/14.
 */
public interface Transformer<A,B> {

    int getId();

    URI getSourceSchema();

    URI getTargetSchema();

    double getProximity();

    B transform(A source);

    A transformBack(B target);
}
