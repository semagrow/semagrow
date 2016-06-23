package org.semagrow.evaluation;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Created by antonis on 21/3/2016.
 */
public interface QueryExecutorResolver {

    Optional<QueryExecutor> resolve(Site site);

}
