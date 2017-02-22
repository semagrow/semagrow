package org.semagrow.plan.queryblock;

import org.semagrow.plan.*;

import java.util.Collection;
import java.util.Set;

/**
 * @author acharal
 * @since 2.0
 */
public interface QueryBlock {

    /**
     * Get the name of the variables that will occur
     * in the output of the block
     * @return the set of the output variables
     */
    Set<String> getOutputVariables();

    /**
     * Get the structure properties that are enforced by this query block on its output
     * @return the structure properties of the output (if any). If the block does not
     *         impose any properties on its output then the returned properties are trivial.
     * @see DataProperties ::isTrivial
     */
    DataProperties getOutputDataProperties();

    /**
     * Infer the fact that the output may contain duplicates.
     * This depends on the {@link OutputStrategy} associated with this block
     * and from its children.
     * @return false if it is guaranteed that the output will not contain duplicates; false otherwise.
     * @see OutputStrategy
     * @see this::setDuplicateStrategy
     */
    boolean hasDuplicates();

    /**
     * Get the {@link OutputStrategy} that is set for this block.
     * <p>
     * If not set, the default is {@link OutputStrategy::PRESERVE}
     *
     * @return the current output strategy
     */
    OutputStrategy getDuplicateStrategy();

    /**
     * Set the {@link OutputStrategy} for this block
     * @param duplicateStrategy the desired output strategy.
     */
    void setDuplicateStrategy(OutputStrategy duplicateStrategy);


    /**
     * Get the interesting properties that are associated
     * with this query block
     * @return the associated interesting properties
     */
    InterestingProperties getInterestingProperties();

    /**
     * Associates a set of interesting properties with this block
     * @param intProps the interesting properties to associate with
     */
    void setInterestingProperties(InterestingProperties intProps);

    <X extends Exception> void visit(QueryBlockVisitor<X> visitor) throws X;

    <X extends Exception> void visitChildren(QueryBlockVisitor<X> visitor) throws X;

    Collection<Plan> getPlans(CompilerContext context);

}
