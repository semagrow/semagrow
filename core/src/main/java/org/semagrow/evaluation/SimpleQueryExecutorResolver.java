package org.semagrow.evaluation;

import org.semagrow.selector.Site;
import java.util.Optional;

/**
 * Created by antonis on 21/3/2016.
 */
public class SimpleQueryExecutorResolver implements QueryExecutorResolver {

    public SimpleQueryExecutorResolver() {}

    @Override
    public Optional<QueryExecutor> resolve(Site endpoint) {

        return QueryExecutorRegistry.getInstance()
                        .get(endpoint.getType())
                        .flatMap(f -> { try {
                            return Optional.of(f.getQueryExecutor(null)); } catch(QueryExecutorConfigException e) {
                            return Optional.empty();
                        }});
    }
}
