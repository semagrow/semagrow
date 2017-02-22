package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * An interface that specifies the functionality of a factory
 * that can create @{code Plan}s.
 * @author acharal
 */
public interface PlanFactory {

    /**
     * Creates a Plan object from a tree of physical {@link TupleExpr} operators.
     * @param physicalExpr the tree of operators
     * @return a {@link Plan} object with attached the properties that can be
     *         derived from the operators of {@code physicalExpr}
     */
    Plan create(TupleExpr physicalExpr);

    /**
     * Creates a Plan object from a tree of physical {@link TupleExpr} operators
     * given a set of initial properties.
     * @param physicalExpr the tree of operators.
     * @param props an initial set of predefined properties
     * @return a {@link Plan} object with attached the properties {@code props} and
     * any derived properties from the operators of {@code physicalExpr}. The derived
     * properties may overwrite any conficting properties in {@code props}
     */
    Plan create(TupleExpr physicalExpr, PlanProperties props);

}
