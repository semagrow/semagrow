package org.semagrow.plan;

import org.semagrow.selector.Site;

/**
 *
 * @author acharal
 */
public interface PlanGenerationContext {

    /**
     * Enforces a {@code site} to a plan {@code p}.
     * This means that checks whether the given plan complies already
     * with the desired site or adds an operator in order to enforce the site.
     * @param p a given query plan
     * @param site a site to enforce
     * @return a Plan derived from {@code p} such that the {@link PlanProperties} contains the enforced {@code site}.
     */
    Plan enforce(Plan p, Site site);

    /**
     * Enforces an {@code ordering} to a plan {@code p}.
     * This means that checks whether the given plan complies already
     * with the desired ordering or adds an operator in order to enforce the ordering.
     * @param p a given query plan
     * @param ordering a site to enforce
     * @return a Plan derived from {@code p} such that the {@link PlanProperties} contains the enforced {@code ordering}.
     */
    Plan enforce(Plan p, Ordering ordering);

}
