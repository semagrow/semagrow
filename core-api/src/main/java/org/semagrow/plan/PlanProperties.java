package org.semagrow.plan;

import org.semagrow.selector.Site;

import java.math.BigInteger;

/**
 * A structure that contains the {@link Plan} properties needed
 * by the {@link PlanOptimizer}.
 *
 * @author acharal
 */
public class PlanProperties {

    private Cost nodeCost;

    private BigInteger cardinality;

    private Site site;

    private DataProperties dataProps;

    public BigInteger getCardinality() { return cardinality; }

    public void setCardinality(BigInteger card) { this.cardinality = card;}

    public Cost getCost() { return nodeCost; }

    public void setCost(Cost cost) { this.nodeCost = cost; }

    public Site getSite() { return site; }

    public void setSite(Site site) { this.site = site; }

    public DataProperties getDataProperties() {
        return dataProps;
    }

    public void setDataProperties(DataProperties dataProps) {
        this.dataProps = dataProps;
    }

    public static PlanProperties defaultProperties() {
        PlanProperties p = new PlanProperties();
        //p.setSite(LocalSite.getInstance());
        p.setCost(new Cost(0));
        return p;
    }

    public PlanProperties clone() {
        PlanProperties p = new PlanProperties();
        p.nodeCost = this.nodeCost;
        p.dataProps = this.dataProps;
        p.site = this.site;
        return p;
    }

    public boolean isComparable(PlanProperties props) {
        return (props.getSite().equals(this.getSite()));
    }

    public boolean isCoveredBy(PlanProperties props) {
        return this.getSite().equals(props.getSite())
                && this.getDataProperties().isCoveredBy(props.getDataProperties())
                && this.getCost().compareTo(props.getCost()) >= 0;
    }

}
