package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SingleSourceExpr;
import eu.semagrow.stack.modules.utils.resourceselector.Measurement;
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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Annotates each Statement with a designated source
 * @author acharal@iit.demokritos.gr
 */
@Deprecated
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

    private class SelectedResourceComparator implements Comparator<SelectedResource> {

        private StatementSourceSelection selection;

        public SelectedResourceComparator(StatementSourceSelection selection) {
            this.selection = selection;
        }

        public int compare(SelectedResource selectedResource, SelectedResource selectedResource2) {
            if (selection.hasLimit) {
                long diff1 = selectedResource.getVol() - selection.limit;
                long diff2 = selectedResource2.getVol() - selection.limit;
                if (diff1 < 0 || diff2 < 0) {
                    if (diff1 > diff2)
                        return 1;
                    else if (diff2 > diff1)
                        return -1;
                }
            }

            return Integer.compare(getPreference(selectedResource), getPreference(selectedResource2));
        }

        private int getPreference(SelectedResource resource) {
            for (Measurement m : resource.getLoadInfo()) {
                if (m.getId() == 1)
                    return m.getValue();
            }
            return 0;
        }
    }

    private URI selectOptimalSource(StatementPattern pattern) {
        long measurement = 10;
        SelectedResource resource;

        List<SelectedResource> resources = resourceSelector.getSelectedResources(pattern, measurement);
        Collections.sort(resources, Collections.reverseOrder(new SelectedResourceComparator(this)));

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
