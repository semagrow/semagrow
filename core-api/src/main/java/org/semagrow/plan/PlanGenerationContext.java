package org.semagrow.plan;

import org.semagrow.selector.Site;

/**
 * Created by angel on 4/4/2016.
 */
public interface PlanGenerationContext {

    Plan enforce(Plan p, Site site);

    Plan enforce(Plan p, Ordering ordering);

}
