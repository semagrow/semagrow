package org.semagrow.plan;

import java.math.BigDecimal;

/**
 * A simple data structure for describing a cost of an execution plan.
 * @author acharal
 */
public class Cost implements Comparable<Cost> {

    private BigDecimal cpuCost;

    private long memoryCost;

    private long ioCost;

    private long networkCost;


    public Cost(double cpu) { this(BigDecimal.valueOf(cpu)); }
    public Cost(BigDecimal cpu) { cpuCost = cpu; }

    private Cost() { }

    @Override
    public int compareTo(Cost o) {
        return this.cpuCost.compareTo(o.cpuCost);
    }

    public Cost add(Cost c) {
        Cost a        = new Cost();
        a.cpuCost     = this.cpuCost.add(c.cpuCost);
        a.memoryCost  = this.memoryCost + c.memoryCost;
        a.ioCost      = this.ioCost + c.ioCost;
        a.networkCost = this.networkCost + c.networkCost;
        return a;
    }

    public Cost multiply(long factor) {
        Cost a        = new Cost();
        a.cpuCost     = this.cpuCost.multiply(BigDecimal.valueOf(factor));
        a.memoryCost  = this.memoryCost  * factor;
        a.ioCost      = this.ioCost  * factor;
        a.networkCost = this.networkCost  * factor;
        return a;
    }

    /**
     * This method returns a cost value that aggregates the various partial
     * cost values (CPU, memory, IO, and network cost.
     *
     * NOTE that currently only CPU cost is used.
     * @return
     */

    public static Cost cpuCost(BigDecimal cpuCost) { return new Cost(cpuCost); }

    public static Cost networkCost(long networkCost) {
        Cost c = new Cost();
        c.networkCost = networkCost;
        return c;
    }

    @Override
    public String toString() {
        return "["+this.cpuCost+","+this.networkCost+"]";
    }
}
