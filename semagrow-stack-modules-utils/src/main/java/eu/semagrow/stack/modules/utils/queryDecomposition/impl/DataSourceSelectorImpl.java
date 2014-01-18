/*
 *
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.ReactivityParameters;
import eu.semagrow.stack.modules.utils.queryDecomposition.DataSourceSelector;
import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openrdf.query.algebra.StatementPattern;

/**
 *
 * @author ggianna
 */
public class DataSourceSelectorImpl implements DataSourceSelector {
    protected ReactivityParameters reactivityParams;
    AlternativeDecomposition alternativeDecompositions = 
            null;
    Map<StatementPattern, List<SelectedResource>> fragmentInfo;

    public DataSourceSelectorImpl(ReactivityParameters reactivityParams) {
        this.reactivityParams = reactivityParams;
        this.fragmentInfo = new HashMap<StatementPattern, 
                List<SelectedResource>>();
    }

    public void setReactivityParameters(ReactivityParameters rpParams) {
        this.reactivityParams = rpParams;
    }
    
    protected void addFragmentInfo(StatementPattern st, 
            List<SelectedResource> resourceInfo) {
        fragmentInfo.put(st, resourceInfo);
    }

    public Map<StatementPattern, List<SelectedResource>> getStatementInfo() {
        return fragmentInfo;
    }

    public void addStatementInfo(StatementPattern st, List<SelectedResource> resourceInfo) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
