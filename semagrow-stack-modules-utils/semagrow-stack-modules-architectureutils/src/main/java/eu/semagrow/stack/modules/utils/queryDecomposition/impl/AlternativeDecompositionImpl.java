/*
 * 
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.queryDecomposition.AlternativeDecomposition;
import eu.semagrow.stack.modules.utils.queryDecomposition.RemoteQueryFragment;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author ggianna
 */
public class AlternativeDecompositionImpl extends 
        ArrayList<RemoteQueryFragment> implements AlternativeDecomposition 
{
    public boolean remove(RemoteQueryFragment f) {
        return this.remove(f);
    }

    public Collection<RemoteQueryFragment> getRemoteQueryFragments() {
        return this;
    }

    
}
