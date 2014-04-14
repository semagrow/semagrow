/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.semagrow.stack.modules.utils.queryDecomposition;

import java.net.URI;
import java.util.List;
import org.openrdf.query.parser.ParsedOperation;

/**
 *
 * @author ggianna
 */
public interface RemoteQueryFragment {
    public ParsedOperation getFragment();
    public String getEquivalentSPARQLQuery();
    public List<URI> getSources();
}
