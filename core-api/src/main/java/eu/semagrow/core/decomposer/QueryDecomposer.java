package eu.semagrow.core.decomposer;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Created by angel on 6/16/14.
 */
public interface QueryDecomposer {

    void decompose(TupleExpr expr, Dataset dataset, BindingSet bindings);

}
