package eu.semagrow.core.eval;

import eu.semagrow.core.source.Site;

/**
 * Created by antonis on 21/3/2016.
 */
public interface QueryExecutorResolver {

    QueryExecutor resolve(Site site);

}
