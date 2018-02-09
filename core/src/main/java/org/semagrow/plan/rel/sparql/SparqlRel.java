package org.semagrow.plan.rel.sparql;

import org.apache.calcite.rel.RelNode;

/**
 * Created by angel on 13/7/2017.
 */
public interface SparqlRel extends RelNode {

    SparqlImplementor.Result implement(SparqlImplementor implementor);

}
