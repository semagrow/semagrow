package org.semagrow.plan.queryblock;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by angel on 13/9/2016.
 */
public class UnionMergeVisitor extends AbstractQueryBlockVisitor<RuntimeException> {

    @Override
    public void meet(UnionBlock unionBlock) {
        super.meet(unionBlock);

        final Collection<QueryBlock> blocks = new ArrayList<>();
        final Collection<QueryBlock> merged = new ArrayList<>();

        unionBlock.getBlocks().stream()
           .forEach( b -> {
                if (b instanceof UnionBlock) {
                    UnionBlock bb = (UnionBlock)b;
                    blocks.addAll(bb.getBlocks());
                    merged.add(bb);
                }
           });
        unionBlock.removeAll(merged);
        unionBlock.addAll(blocks);
    }

}
