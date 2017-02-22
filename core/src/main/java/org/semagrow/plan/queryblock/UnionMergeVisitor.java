package org.semagrow.plan.queryblock;

/**
 * Created by angel on 13/9/2016.
 */
public class UnionMergeVisitor extends AbstractQueryBlockVisitor<RuntimeException> {

    @Override
    public void meet(UnionBlock unionBlock) {
        super.meet(unionBlock);
        unionBlock.getBlocks().stream()
           .forEach( b -> {
                if (b instanceof UnionBlock) {
                    UnionBlock bb = (UnionBlock)b;
                    unionBlock.addAll(bb.getBlocks());
                }
           });
    }

}
