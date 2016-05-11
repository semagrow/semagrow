package eu.semagrow.core.plan;

import eu.semagrow.core.source.Site;

/**
 * Created by angel on 4/4/2016.
 */
public interface PlanGenerationContext {

    Plan enforce(Plan p, Site site);

    Plan enforce(Plan p, Ordering ordering);

}
