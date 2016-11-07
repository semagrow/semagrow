package org.semagrow.plan.operators;

import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupElem;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Created by angel on 1/10/2016.
 */
public class StreamGroup extends Group {

    public StreamGroup() {
        super();
    }

    public StreamGroup(TupleExpr arg) {
        super(arg);
    }

    public StreamGroup(TupleExpr arg, Iterable<String> groupBindingNames) {
        super(arg, groupBindingNames);
    }

    public StreamGroup(TupleExpr arg, Iterable<String> groupBindingNames, Iterable<GroupElem> groupElements) {
        super(arg, groupBindingNames, groupElements);
    }

}
