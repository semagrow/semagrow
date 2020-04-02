package org.semagrow.plan.rel.sparql;

import org.apache.calcite.plan.RelImplementor;
import org.apache.calcite.rel.RelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Created by angel on 15/7/2017.
 */
public interface SparqlImplementor extends RelImplementor {

    Result implement(RelNode node);


    interface Result {

        /*
        TupleExpr root;

        TupleExpr getExpr()
        */

        String asQueryString();

        TupleExpr asTupleExpr();

    }
}
