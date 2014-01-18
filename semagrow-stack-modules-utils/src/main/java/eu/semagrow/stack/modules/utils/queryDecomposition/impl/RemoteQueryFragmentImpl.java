/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.semagrow.stack.modules.utils.queryDecomposition.impl;

import eu.semagrow.stack.modules.utils.queryDecomposition.RemoteQueryFragment;
import java.util.List;
import org.openrdf.model.URI;
import org.openrdf.query.parser.ParsedOperation;

/** Represents a single query fragment, that needs/can be executed
 * on a variety of remote endpoints (sources).
 *
 * @author ggianna
 */
public class RemoteQueryFragmentImpl implements RemoteQueryFragment {

    protected ParsedOperation fragment;
    protected List<URI> Sources;

    public RemoteQueryFragmentImpl(ParsedOperation fragment, List<URI> Sources) {
        this.fragment = fragment;
        this.Sources = Sources;
    }
    

    public ParsedOperation getFragment() {
        return fragment;
    }

    public List<URI> getSources() {
        return Sources;
    }

}
