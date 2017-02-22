package org.semagrow.statistics;

import org.eclipse.rdf4j.repository.Repository;
import org.semagrow.selector.Site;

import java.util.Optional;

/**
 * Created by angel on 15/6/2016.
 */
public class VOIDStatisticsProvider implements StatisticsProvider {

    private Repository repo;

    public VOIDStatisticsProvider(Repository repo) {
        this.repo = repo;
    }

    public Optional<Statistics> getStatistics(Site site) {
        return Optional.of(new VOIDStatistics(site, repo));
    }

}
