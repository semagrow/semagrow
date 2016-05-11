package eu.semagrow.core.plan;


import eu.semagrow.core.source.LocalSite;
import eu.semagrow.core.source.Site;

/**
 * A structure that contains the @{link Plan} properties needed
 * by the {@link PlanOptimizer}.
 *
 * @author Angelos Charalambidis
 */
public class PlanProperties {

    private Cost nodeCost;

    private Cost cumulativeCost;

    private Ordering ordering;

    private long cardinality;

    private Site site;

    public long getCardinality() { return cardinality; }

    public void setCardinality(long card) { this.cardinality = card;}

    public Cost getCost() { return nodeCost; }

    public void setCost(Cost cost) { this.nodeCost = cost; }

    public Site getSite() { return site; }

    public void setSite(Site site) { this.site = site; }

    /*
    public Collection<URI> getSchemas(String var) {
        if (schemas.containsKey(var))
            return schemas.get(var);

        return Collections.emptySet();
    }

    public void setSchemas(String var, Collection<URI> varSchemas) {
        this.schemas.put(var, varSchemas);
    }
    */

    public Ordering getOrdering() { return ordering; }

    public void setOrdering(Ordering ordering) { this.ordering = ordering; }


    public static PlanProperties defaultProperties() {
        PlanProperties p = new PlanProperties();
        p.setOrdering(Ordering.NOORDERING);
        p.setSite(LocalSite.getInstance());
        return p;
    }

    public PlanProperties clone() {
        PlanProperties p = new PlanProperties();
        p.nodeCost = this.nodeCost;
        p.ordering = this.ordering;
        p.site = this.site;
        return p;
    }

    public boolean isComparable(PlanProperties properties) {
        return (properties.getSite().equals(this.getSite()));
    }
}
