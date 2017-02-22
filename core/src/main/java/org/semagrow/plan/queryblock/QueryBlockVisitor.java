package org.semagrow.plan.queryblock;

/**
 * Visitor of a {@link QueryBlock} tree
 * @author acharal
 * @since 2.0
 */
public interface QueryBlockVisitor<X extends Exception> {

    void meet(QueryBlock b) throws X;

}
