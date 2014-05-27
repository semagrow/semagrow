package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.querydecomp.SourceMetadata;
import eu.semagrow.stack.modules.querydecomp.SourceSelector;
import org.openrdf.query.algebra.StatementPattern;

import java.util.List;

/**
 * Created by angel on 5/27/14.
 */
public class VOIDSourceSelector implements SourceSelector {


    public List<SourceMetadata> getSources(StatementPattern pattern) {
        return null;
    }
}
