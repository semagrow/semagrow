package org.semagrow.plan.rel.sparql;

public class SparqlDialect {

    private SparqlVersion version;
    private DatabaseProduct databaseProduct;

    SparqlDialect(SparqlVersion version) {
        this.version = version;
        this.databaseProduct = DatabaseProduct.UNKNOWN;
    }

    static public SparqlDialect create() {
        return new SparqlDialect(SparqlVersion.SPARQL10);
    }

    public SparqlVersion getVersion() {
        return version;
    }

    @Override
    public String toString() { return getVersion().toString(); }

    public DatabaseProduct getDatabaseProduct() { return databaseProduct; }

    public boolean supportsFeature(SparqlFeature op) {

        return op != null
                && (op.isStandard()
                && getVersion().isBackwardsCompatible(op.getRequiredSparqlVersion()));
    }

    /**
     * Supports nested queries
     * @return
     */
    public boolean supportsSubQuery() {
        return version == SparqlVersion.SPARQL11;
    }

    /**
     * Supports federated queries
     * @return
     */
    public boolean supportsFederatedQuery() {
        return version == SparqlVersion.SPARQL11;
    }

    public boolean isCompatible(SparqlDialect dialect) { return this == dialect; }

    enum DatabaseProduct {
        VIRTUOSO,
        FOURSTORE,
        UNKNOWN;

        public SparqlDialect getDialect() {
            return new SparqlDialect(SparqlVersion.SPARQL10);
        }
    }


}
