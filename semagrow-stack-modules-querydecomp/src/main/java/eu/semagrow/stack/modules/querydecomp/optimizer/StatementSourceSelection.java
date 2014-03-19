package eu.semagrow.stack.modules.querydecomp.optimizer;

import eu.semagrow.stack.modules.querydecomp.algebra.SingleSourceExpr;
import eu.semagrow.stack.modules.utils.resourceselector.ResourceSelector;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import org.openrdf.model.URI;
import java.util.List;

/**
 * Created by angel on 3/14/14.
 */
public class StatementSourceSelection extends QueryModelVisitorBase<RuntimeException>
        implements QueryOptimizer  {

    private ResourceSelector resourceSelector;
    private long limit;
    private long offset;
    private boolean hasLimit = false;

    public StatementSourceSelection(ResourceSelector resourceSelector){
        this.resourceSelector = resourceSelector;
    }

    @Override
    public void meet(StatementPattern pattern) {
        URI source = selectOptimalSource(pattern);
        SingleSourceExpr annotated = new SingleSourceExpr(pattern.clone(), source);
        pattern.replaceWith(annotated);
    }

    @Override
    public void meet(Slice slice) {
        hasLimit = true;
        limit = slice.getLimit();
        offset = slice.getOffset();
        slice.getArg().visit(this);
    }

    private URI selectOptimalSource(StatementPattern pattern) {
        long measurement = 10;
        SelectedResource resource;

        List<SelectedResource> resources = resourceSelector.getSelectedResources(pattern, measurement);

        if (resources.iterator().hasNext()) {
            resource = resources.iterator().next();
            return resource.getEndpoint();
        }

        return null;
    }

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new StatementSourceSelection(resourceSelector));
    }
}
