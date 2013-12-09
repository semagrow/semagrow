/*
 *
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.queryDecomposition.DataSourceSelector;
import eu.semagrow.stack.modules.utils.queryDecomposition.DistributedExecutionPlan;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;

/**
 *
 * @author ggianna
 */
public class DataSourceSelectorImpl implements DataSourceSelector {
    protected ReactivityParameters reactivityParams;
    List<DistributedExecutionPlan> plans = 
            null;
    Map<StatementPattern, List<SelectedResource>> fragmentInfo;
    Map<StatementPattern, Integer> fragmentPlanIndex;

    public DataSourceSelectorImpl(ReactivityParameters reactivityParams) {
        this.reactivityParams = reactivityParams;
        this.fragmentInfo = new HashMap<StatementPattern, 
                List<SelectedResource>>();
    }

    public void setReactivityParameters(ReactivityParameters rpParams) {
        this.reactivityParams = rpParams;
    }

    public List<DistributedExecutionPlan> getPlans(String sQuery) {
        if (plans == null)
            calculatePlans(sQuery);
        return plans;
    }
    
    protected void calculatePlans(String sQuery) {
        plans = new ArrayList<DistributedExecutionPlan>();
        int iCnt = 0;

        // For each segment
        for (StatementPattern sp : fragmentInfo.keySet()) {
            // Init plan counter
            fragmentPlanIndex.put(sp, 0);
        }
        
        // TODO: Should do for several plans
        // Rewrite query here
        String sQueryToChange = new String(sQuery);
        // For each segment
        for (StatementPattern sp : fragmentInfo.keySet()) {
            // if you find the pattern as part of the query
            if (sQueryToChange.contains(sp.getSignature())) {
                int iCurPlanForStatement = fragmentPlanIndex.get(sp);
                // String source for selected statement
                URI uSource = fragmentInfo.get(sp).get(iCurPlanForStatement
                    ).getEndpoint();
                // Increase plan alternative
                // TODO: Check if we have reached the end
                fragmentPlanIndex.put(sp, iCurPlanForStatement + 1);
                
                
                // replace it, adding the service keyword
                sQueryToChange = sQueryToChange.replaceAll(sp.getSignature(), 
                        String.format("SERVICE %s {%s}", uSource.toString(),
                                sp.getSignature()));
            }
        }
        
        // Add plan to list
        final String sQueryFinal = sQueryToChange;
        plans.add(new DistributedExecutionPlan() {

            public String getQuery() {
                return sQueryFinal;
            }
        });
       
    }
    
    public void addFragmentInfo(StatementPattern st, 
            List<SelectedResource> resourceInfo) {
        fragmentInfo.put(st, resourceInfo);
    }
}
