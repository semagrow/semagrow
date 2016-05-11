package eu.semagrow.core.plan;

/**
 * A simple data structure for describing a cost of an execution plan.
 * @author Angelos Charalambidis
 */
public class Cost implements Comparable<Cost> {

    private double cpuCost;

    private long memoryCost;

    private long ioCost;

    private long networkCost;


    public Cost(double cpu) { cpuCost = cpu; }

    private Cost() { }

    @Override
    public int compareTo(Cost o) {
        if (this.cpuCost < o.cpuCost)
            return -1;
        else if (this.cpuCost > o.cpuCost)
            return 1;
        else
            return 0;
    }

    public Cost add(Cost c) {
        Cost a        = new Cost();
        a.cpuCost     = this.cpuCost + c.cpuCost;
        a.memoryCost  = this.memoryCost + c.memoryCost;
        a.ioCost      = this.ioCost + c.ioCost;
        a.networkCost = this.networkCost + c.networkCost;
        return a;
    }

    public Cost multiply(long factor) {
        Cost a        = new Cost();
        a.cpuCost     = this.cpuCost * factor;
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

    public double getOverallCost() { return this.cpuCost; }

    public static Cost cpuCost(double cpuCost) { return new Cost(cpuCost); }

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
