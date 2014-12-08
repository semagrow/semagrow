package eu.semagrow.stack.modules.alignment;

import org.openrdf.model.URI;

/**
 * Created by angel on 12/2/14.
 */
public class InvertTransformer<A,B> implements Transformer<A,B> {

    private Transformer<B,A> forwardTransformer;

    public InvertTransformer(Transformer<B,A> transformer) {
        assert transformer != null;
        forwardTransformer = transformer;
    }

    @Override
    public int getId() {
        return -forwardTransformer.getId();
    }

    @Override
    public URI getSourceSchema() {
        return forwardTransformer.getTargetSchema();
    }

    @Override
    public URI getTargetSchema() {
        return forwardTransformer.getSourceSchema();
    }

    @Override
    public double getProximity() {
        return forwardTransformer.getProximity();
    }

    @Override
    public B transform(A source) {
        return forwardTransformer.transformBack(source);
    }

    @Override
    public A transformBack(B target) {
        return forwardTransformer.transform(target);
    }
}
