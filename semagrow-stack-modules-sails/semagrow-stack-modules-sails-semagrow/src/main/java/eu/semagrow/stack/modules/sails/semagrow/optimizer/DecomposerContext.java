package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.api.source.SourceMetadata;
import org.openrdf.query.algebra.ValueExpr;

import java.util.Collection;

/**
 * Created by angel on 10/8/14.
 */
public class DecomposerContext {

    Ordering ordering;

    Collection<ValueExpr> filters;

    int limit;

    Collection<SourceMetadata> sourceMetadata;

}
