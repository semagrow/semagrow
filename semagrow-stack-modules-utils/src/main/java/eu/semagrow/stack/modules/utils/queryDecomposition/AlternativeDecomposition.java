/*
 *
 */

package eu.semagrow.stack.modules.utils.queryDecomposition;

import java.util.Collection;

/**
 *
 * @author ggianna
 */
public interface AlternativeDecomposition extends Iterable<RemoteQueryFragment> {
    public boolean add(RemoteQueryFragment f);
    public boolean remove(RemoteQueryFragment f);
    public Collection<RemoteQueryFragment> getRemoteQueryFragments();
}
