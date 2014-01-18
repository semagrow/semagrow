/*
 * 
 */

package eu.semagrow.stack.modules.utils.queryDecomposition;

import java.util.Collection;

/**
 *
 * @author ggianna
 */
public interface DecompositionStrategySelector {
   public Collection<AlternativeDecomposition> rankDecompositions(
           Collection<AlternativeDecomposition> possibleDecompositions);
}
