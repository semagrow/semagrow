package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.api.statistics.Statistics;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.Repository;

import java.util.*;

/**
 * Created by angel on 4/30/14.
 */
public class VOIDStatistics extends VOIDBase implements Statistics {


    public VOIDStatistics(Repository voidRepository) {
        super(voidRepository);
    }

    public long getDistinctObjects(StatementPattern pattern, URI source) {
        Value pVal = pattern.getPredicateVar().getValue();
        Value sVal = pattern.getSubjectVar().getValue();
        Value oVal = pattern.getObjectVar().getValue();

        Set<Resource> datasets  = getMatchingDatasetsOfEndpoint(source);

        if (datasets.isEmpty())
            return 0;

        if (oVal != null)
            return 1;

        Set<Resource> pDatasets = new HashSet<Resource>(datasets);
        Set<Resource> sDatasets = new HashSet<Resource>(datasets);

        if (pVal != null)
            pDatasets.retainAll(getMatchingDatasetsOfPredicate((URI) pVal));

        if (sVal != null && sVal instanceof URI)
            sDatasets.retainAll(getMatchingDatasetsOfSubject((URI)sVal));

        Set<Resource> spDatasets = new HashSet<Resource>(pDatasets);
        spDatasets.retainAll(sDatasets);

        if (!spDatasets.isEmpty()) { // datasets that match both the predicate and subject
            return getDistinctObjects(spDatasets);
        } else if (pVal != null && !pDatasets.isEmpty()) {
            return getDistinctObjects(pDatasets);
        } else if (sVal != null && !sDatasets.isEmpty()) {
            return getDistinctObjects(sDatasets);
        }
        else {
            return getDistinctObjects(datasets);
        }
    }

    public long getDistinctSubjects(StatementPattern pattern, URI source) {
        Value pVal = pattern.getPredicateVar().getValue();
        Value sVal = pattern.getSubjectVar().getValue();

        Set<Resource> datasets  = getMatchingDatasetsOfEndpoint(source);

        if (datasets.isEmpty())
            return 0;

        if (sVal != null)
            return 1;

        Set<Resource> pDatasets = new HashSet<Resource>(datasets);

        if (pVal != null && pVal instanceof URI)
            pDatasets.retainAll(getMatchingDatasetsOfPredicate((URI)pVal));

        //TODO: check datasets that must subject uriRegexPattern
        //

        if (!pDatasets.isEmpty()) {
            return getDistinctSubjects(pDatasets);
        }else{
            return getDistinctSubjects(datasets);
        }
    }

    public long getDistinctPredicates(StatementPattern pattern, URI source){
        Value pVal = pattern.getPredicateVar().getValue();
        Value sVal = pattern.getSubjectVar().getValue();

        Set<Resource> datasets  = getMatchingDatasetsOfEndpoint(source);

        if (datasets.isEmpty())
            return 0;

        if (pVal != null)
            return 1;


        Set<Resource> sDatasets = new HashSet<Resource>(datasets);


        if (sVal != null && sVal instanceof URI)
            sDatasets.retainAll(getMatchingDatasetsOfSubject((URI)sVal));


        if (sVal != null && !sDatasets.isEmpty()) {
            return getDistinctPredicates(sDatasets);
        }else{
            return getDistinctPredicates(datasets);
        }
    }

    public long getPatternCount(StatementPattern pattern, URI source) {

        Value sVal = pattern.getSubjectVar().getValue();
        Value pVal = pattern.getPredicateVar().getValue();
        Value oVal = pattern.getObjectVar().getValue();

        if (sVal != null && pVal != null && oVal != null)
            return 1;

        Set<Resource> datasets  = getMatchingDatasetsOfEndpoint(source);

        //Set<Resource> oDatasets = new HashSet<Resource>(datasets);

        if (datasets.isEmpty())
            return 0;

        Set<Resource> pDatasets = new HashSet<Resource>(datasets);

        Set<Resource> sDatasets = new HashSet<Resource>(datasets);

        if (pVal != null)
            pDatasets.retainAll(getMatchingDatasetsOfPredicate((URI) pVal));

        if (sVal != null && sVal instanceof URI)
            sDatasets.retainAll(getMatchingDatasetsOfSubject((URI)sVal));

        Set<Resource> spDatasets = new HashSet<Resource>(pDatasets);
        spDatasets.retainAll(sDatasets);

        if (!spDatasets.isEmpty()) { // datasets that match both the predicate and subject
            long d = 1;
            if (oVal != null)
                d = getDistinctObjects(spDatasets);
            return getTriplesCount(spDatasets) / d;
        } else if (pVal != null && !pDatasets.isEmpty()) {
            long d = 1;
            if (oVal != null)
                d *= getDistinctObjects(pDatasets);
            if (sVal != null)
                d *= getDistinctSubjects(pDatasets);

            return getTriplesCount(pDatasets) / d;
        } else if (sVal != null && !sDatasets.isEmpty()) {
            long d = 1;
            if (oVal != null)
                d *= getDistinctObjects(sDatasets);
            if (pVal != null)
                d *= getDistinctPredicates(sDatasets);

            return getTriplesCount(sDatasets) / d;
        }
        else {
            long d = 1;

            if (oVal != null)
                d *= getDistinctObjects(datasets);
            if (pVal != null)
                d *= getDistinctPredicates(datasets);
            if (sVal != null)
                d *= getDistinctSubjects(datasets);

            if (d > 0 )
                return getTriplesCount(datasets) / d;
            else
                return 0;
        }
    }

    public long getTriplesCount(URI source) {

        // get all triples statistics from datasets with property sparqlEndpoint = source
        // and get the maximum
        return 0;
    }

    private Long getTriplesCount(Collection<Resource> datasets) {
        long triples = 0;
        boolean realData = false;
        for (Resource dataset : datasets) {
            Long t = getTriples(dataset);
            if (t != null && triples <= t.longValue()) {
                realData = true;
                triples = t.longValue();
            }
        }
        if (realData)
            return triples;
        else
            return (long)0;
    }

    private Long getDistinctSubjects(Collection<Resource> datasets) {
        long triples = 0;
        int i = 0;
        for (Resource dataset : datasets) {
            Long t = getDistinctSubjects(dataset);
            if (t != null) {
                triples += t.longValue();
                i++;
            }
        }
        return (i == 0) ? 1 : triples/i;
    }

    private Long getDistinctObjects(Collection<Resource> datasets) {
        long triples = 0;
        int i = 0;
        for (Resource dataset : datasets) {
            Long t = getDistinctObjects(dataset);
            if (t != null) {
                triples += t.longValue();
                i++;
            }
        }
        return (i==0) ? 1 : triples/i;
    }

    private Long getDistinctPredicates(Collection<Resource> datasets) {
        long triples = 0;
        int i = 0;
        for (Resource dataset : datasets) {
            Long t = getDistinctPredicates(dataset);
            if (t != null) {
                triples += t.longValue();
                i++;
            }
        }
        return (i == 0) ? 1 : triples/i;
    }
}
