package org.semagrow.plan;

import org.semagrow.selector.Site;

/**
 * Created by angel on 14/6/2016.
 */
public interface PlanProperties {

    long getCardinality();

    void setCardinality(long card);

    Cost getCost();

    void setCost(Cost cost);

    Site getSite();

    void setSite(Site site);

    Ordering getOrdering();

    void setOrdering(Ordering o);

    boolean isComparable(PlanProperties properties);

    PlanProperties clone();
}
