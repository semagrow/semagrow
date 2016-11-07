package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.Join;

import java.util.Collection;

/**
 * The interface of a generator that knows how to generate a specific
 * physical join operation. This includes knowledge of the plan properties
 * of the join arguments needed by the specific implementation of the join.
 * @author acharal
 */
public interface JoinImplGenerator {

    /**
     * Generates a collection of implementation specific {@link Join} trees
     * @param p1 an execution plan of the left operant
     * @param p2 an execution plan of the right operant
     * @param ctx the context of the plan generation
     * @return a possible empty collection of join trees
     */
    Collection<Join> generate(Plan p1, Plan p2, PlanGenerationContext ctx);

}
