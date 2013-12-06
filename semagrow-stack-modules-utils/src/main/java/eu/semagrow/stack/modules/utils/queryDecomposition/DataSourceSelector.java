/*
 *
 */

package eu.semagrow.stack.modules.utils.queryDecomposition;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.SelectedResource;
import java.util.List;
import org.openrdf.query.algebra.StatementPattern;

/**
 *
 * @author ggianna
 */
public interface DataSourceSelector {
    public void setReactivityParameters(ReactivityParameters rpParams);
    public List<DistributedExecutionPlan> getPlans(String sQuery);
    public void addFragmentInfo(StatementPattern st, 
            List<SelectedResource> resourceInfo);  
}
