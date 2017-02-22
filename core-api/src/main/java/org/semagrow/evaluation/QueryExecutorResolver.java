package org.semagrow.evaluation;

import org.semagrow.selector.Site;

import java.util.Optional;

/**
 *
 * @author acharal
 * @since 2.0
 */
public interface QueryExecutorResolver {

    Optional<QueryExecutor> resolve(Site site);

}
