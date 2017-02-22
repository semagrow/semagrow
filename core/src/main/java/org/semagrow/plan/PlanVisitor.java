package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.semagrow.plan.operators.*;

/**
 * A visitor of query {@link Plan}s that contain physical nodes
 * instead of logical.
 * @see QueryModelVisitor
 * @author acharal
 */
public interface PlanVisitor<X extends Exception> extends QueryModelVisitor<X> {

    void meet(Plan p) throws X;

    void meet(BindJoin j) throws X;

    void meet(HashJoin j) throws X;

    void meet(MergeJoin j) throws X;

    void meet(MergeUnion u) throws X;

    void meet(SourceQuery q) throws X;
}
