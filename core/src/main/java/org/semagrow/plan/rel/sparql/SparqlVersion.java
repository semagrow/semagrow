package org.semagrow.plan.rel.sparql;


public enum SparqlVersion implements Comparable<SparqlVersion> {

    SPARQL10(1,0),
    SPARQL11(1,1);

    private int major;
    private int minor;

    SparqlVersion(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }

    public boolean isBackwardsCompatible(SparqlVersion that)  {
        return this.major == that.major && this.minor >= that.minor;
    }

    @Override
    public String toString() {
        if (major == 1 && minor == 0)
            return "SPARQL";
        else
            return "SPARQL" + major + "." + minor;
    }

}
