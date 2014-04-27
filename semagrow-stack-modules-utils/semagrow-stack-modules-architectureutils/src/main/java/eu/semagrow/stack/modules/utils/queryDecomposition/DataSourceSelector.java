/*
 *
 */

package eu.semagrow.stack.modules.utils.queryDecomposition;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.api.SelectedResource;

import java.util.List;
import java.util.Map;

import org.openrdf.query.algebra.StatementPattern;

/**
 *
 * @author ggianna
 */
public interface DataSourceSelector {
    public void setReactivityParameters(ReactivityParameters rpParams);
    public Map<StatementPattern, List<SelectedResource>> getStatementInfo();
    public void addStatementInfo(StatementPattern st, 
            List<SelectedResource> resourceInfo);  
}
