package org.semagrow.model;


import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.AbstractValueFactory;

/**
 * Semagrow ValueFactory overrides the default behaviour of the SimpleValueFactory
 * by creating a SimplePlainLiteral instead of a SimpleLiteral when no type is given.
 * Created by angel on 8/6/2016.
 */
public class SemagrowValueFactory extends AbstractValueFactory {

    private static final SemagrowValueFactory sharedInstance = new SemagrowValueFactory();


    @Override
    public Literal createLiteral(String value) {
        return new SimplePlainLiteral(value);
    }

    public static ValueFactory getInstance() { return sharedInstance; }
}
