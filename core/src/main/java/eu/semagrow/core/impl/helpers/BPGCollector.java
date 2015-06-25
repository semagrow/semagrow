package eu.semagrow.core.impl.helpers;

import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by angel on 4/26/14.
 */
public class BPGCollector extends QueryModelVisitorBase<RuntimeException> {

    private TupleExpr lastBGPNode;

    private Collection<TupleExpr> bgps = new LinkedList<TupleExpr>();

    private BPGCollector() { }

    public static Collection<TupleExpr> process(TupleExpr expr){
        BPGCollector bpgCollector = new BPGCollector();
        expr.visit(bpgCollector);
        return bpgCollector.bgps;
    }

    // --------------------------------------------------------------

    /**
     * Handles binary nodes with potential BGPs as children (e.g. union, left join).
     */
    @Override
    public void meetBinaryTupleOperator(BinaryTupleOperator node) throws RuntimeException {

        for (TupleExpr expr : new TupleExpr[] { node.getLeftArg(), node.getRightArg() }) {
            expr.visit(this);
            if (lastBGPNode != null) {
                // child is a BGP node but this node is not
                this.bgps.add(lastBGPNode);
                lastBGPNode = null;
            }
        }
    }

    /**
     * Handles unary nodes with a potential BGP as child (e.g. projection).
     */
    @Override
    public void meetUnaryTupleOperator(UnaryTupleOperator node) throws RuntimeException {

        node.getArg().visit(this);

        if (lastBGPNode != null) {
            // child is a BGP node but this node is not
            this.bgps.add(lastBGPNode);
            lastBGPNode = null;
        }
    }

    /**
     * Handles statement patterns which are always a valid BGP node.
     */
    @Override
    public void meet(StatementPattern node) throws RuntimeException {
        this.lastBGPNode = node;
    }

    @Override
    public void meet(Filter filter) throws RuntimeException {
        // visit the filter argument but ignore the filter condition
        filter.getArg().visit(this);

        if (lastBGPNode != null) {
            // child is a BGP as well as the filter
            lastBGPNode = filter;
        }
    }

    @Override
    public void meet(Join join) throws RuntimeException {

        boolean valid = true;

        // visit join arguments and check that all are valid BGPS
        for (TupleExpr expr : new TupleExpr[] { join.getLeftArg(), join.getRightArg() }) {
            expr.visit(this);
            if (lastBGPNode == null) {
                // child is not a BGP -> join is not a BGP
                valid = false;
            } else {
                if (!valid) {
                    // last child is a BGP but another child was not
                    this.bgps.add(lastBGPNode);
                    lastBGPNode = null;
                }
            }
        }
        if (valid)
            lastBGPNode = join;
    }
}
