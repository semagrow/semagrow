package org.semagrow.model;

import org.eclipse.rdf4j.model.impl.SimpleLiteral;

/**
 * Created by angel on 8/6/2016.
 */
public class SimplePlainLiteral extends SimpleLiteral implements PlainLiteral {

    protected SimplePlainLiteral(String label) {
        super(label);
    }

    @Override
    public String toString() {
        String label = getLabel();
        StringBuilder sb = new StringBuilder(label.length() * 2);
        sb.append('\"');
        sb.append(label);
        sb.append('\"');
        return sb.toString();
    }
}
