package org.semagrow.plan;


import org.semagrow.local.LocalSite;
import org.semagrow.selector.Site;

/**
 * A structure that contains the @{link Plan} properties needed
 * by the {@link PlanOptimizer}.
 *
 * @author Angelos Charalambidis
 */
public class SimplePlanProperties implements PlanProperties {

    private Cost nodeCost;

    private Cost cumulativeCost;

    private Ordering ordering;

    private long cardinality;

    private Site site;

    @Override
    public long getCardinality() { return cardinality; }

    public void setCardinality(long card) { this.cardinality = card;}

    @Override
    public Cost getCost() { return nodeCost; }

    public void setCost(Cost cost) { this.nodeCost = cost; }

    @Override
    public Site getSite() { return site; }

    public void setSite(Site site) { this.site = site; }

    public Ordering getOrdering() { return ordering; }

    public void setOrdering(Ordering ordering) { this.ordering = ordering; }

    public static SimplePlanProperties defaultProperties() {
        SimplePlanProperties p = new SimplePlanProperties();
        p.setOrdering(Ordering.NOORDERING);
        p.setSite(LocalSite.getInstance());
        p.setCost(new Cost(0));
        return p;
    }

    public PlanProperties clone() {
        SimplePlanProperties p = new SimplePlanProperties();
        p.nodeCost = this.nodeCost;
        p.ordering = this.ordering;
        p.site = this.site;
        return p;
    }

    @Override
    public boolean isComparable(PlanProperties properties) {
        return (properties.getSite().equals(this.getSite()));
    }
}
