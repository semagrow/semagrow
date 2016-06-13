package eu.semagrow.core.impl.statistics;

import eu.semagrow.core.impl.selector.VOIDBase;
import eu.semagrow.core.statistics.Statistics;
import eu.semagrow.core.statistics.StatisticsProvider;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.Repository;

import java.util.*;

/**
 * Created by angel on 4/30/14.
 */
public class VOIDStatisticsProvider extends VOIDBase implements StatisticsProvider {

    public VOIDStatisticsProvider(Repository voidRepository) {
        super(voidRepository);
    }

    public long getDistinctObjects(StatementPattern pattern, IRI source) {
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
            pDatasets.retainAll(getMatchingDatasetsOfPredicate((IRI) pVal));

        if (sVal != null && sVal instanceof IRI)
            sDatasets.retainAll(getMatchingDatasetsOfSubject((IRI)sVal));

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

    public long getDistinctSubjects(StatementPattern pattern, IRI source) {
        Value pVal = pattern.getPredicateVar().getValue();
        Value sVal = pattern.getSubjectVar().getValue();

        if (isTypeClass(pattern)) {
            Set<Resource> datasets = getMatchingDatasetsOfClass(source);
            if (!datasets.isEmpty()) {
                return getEntities(datasets);
            }
        }

        Set<Resource> datasets  = getMatchingDatasetsOfEndpoint(source);

        if (datasets.isEmpty())
            return 0;

        if (sVal != null)
            return 1;

        Set<Resource> pDatasets = new HashSet<Resource>(datasets);

        if (pVal != null && pVal instanceof IRI)
            pDatasets.retainAll(getMatchingDatasetsOfPredicate((IRI)pVal));

        //TODO: check datasets that must subject uriRegexPattern
        //

        if (!pDatasets.isEmpty()) {
            return getDistinctSubjects(pDatasets);
        }else{
            return getDistinctSubjects(datasets);
        }
    }

    public long getDistinctPredicates(StatementPattern pattern, IRI source){
        Value pVal = pattern.getPredicateVar().getValue();
        Value sVal = pattern.getSubjectVar().getValue();

        Set<Resource> datasets  = getMatchingDatasetsOfEndpoint(source);

        if (datasets.isEmpty())
            return 0;

        if (pVal != null)
            return 1;


        Set<Resource> sDatasets = new HashSet<Resource>(datasets);


        if (sVal != null && sVal instanceof IRI)
            sDatasets.retainAll(getMatchingDatasetsOfSubject((IRI)sVal));


        if (sVal != null && !sDatasets.isEmpty()) {
            return getDistinctPredicates(sDatasets);
        }else{
            return getDistinctPredicates(datasets);
        }
    }

    @Override
    public Statistics getStats(final StatementPattern pattern, BindingSet bindings, final IRI source) {
        return new StatisticsImpl(pattern,
                getPatternCount(pattern, source),
                getDistinctSubjects(pattern, source),
                getDistinctPredicates(pattern, source),
                getDistinctObjects(pattern,source));
    }

    public long getPatternCount(StatementPattern pattern, IRI source) {

        Value sVal = pattern.getSubjectVar().getValue();
        Value pVal = pattern.getPredicateVar().getValue();
        Value oVal = pattern.getObjectVar().getValue();

        if (sVal != null && pVal != null && oVal != null)
            return 1;


        if (isTypeClass(pattern)) {
            Set<Resource> datasets = getMatchingDatasetsOfClass((IRI)pattern.getObjectVar().getValue());
            if (!datasets.isEmpty()) {
                return getEntities(datasets);
            }
        }

        Set<Resource> datasets  = getMatchingDatasetsOfEndpoint(source);

        //Set<Resource> oDatasets = new HashSet<Resource>(datasets);

        if (datasets.isEmpty())
            return 0;

        Set<Resource> pDatasets = new HashSet<Resource>(datasets);

        Set<Resource> sDatasets = new HashSet<Resource>(datasets);

        if (pVal != null)
            pDatasets.retainAll(getMatchingDatasetsOfPredicate((IRI) pVal));

        if (sVal != null && sVal instanceof IRI)
            sDatasets.retainAll(getMatchingDatasetsOfSubject((IRI)sVal));

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

    public long getTriplesCount(IRI source) {

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

    private Long getEntities(Collection<Resource> datasets) {
        long triples = 0;
        int i = 0;
        for (Resource dataset : datasets) {
            Long t = getEntities(dataset);
            if (t != null) {
                triples += t.longValue();
                i++;
            }
        }
        return (i == 0) ? 1 : triples/i;
    }

    private boolean isTypeClass(StatementPattern pattern) {
        Value predVal = pattern.getPredicateVar().getValue();
        Value objVal = pattern.getObjectVar().getValue();

        if (predVal != null && objVal != null && predVal.equals(RDF.TYPE))
            return true;
        else
            return false;
    }

    private class StatisticsImpl implements Statistics {

        private StatementPattern pattern;
        private long patternCount;
        private long distinctSubjects;
        private long distinctPredicates;
        private long distinctObjects;

        public StatisticsImpl(StatementPattern pattern,
                              long patternCount,
                              long distinctSubjects,
                              long distinctPredicates,
                              long distinctObjects)
        {
            this.pattern = pattern;
            this.patternCount = patternCount;
            this.distinctSubjects = distinctSubjects;
            this.distinctPredicates = distinctPredicates;
            this.distinctObjects = distinctObjects;
        }

        @Override
        public long getCardinality() { return patternCount; }

        @Override
        public long getVarCardinality(String var) {
            if (pattern.getSubjectVar().getName().equals(var))
                return distinctSubjects;

            if (pattern.getPredicateVar().getName().equals(var))
                return distinctPredicates;

            if (pattern.getObjectVar().getName().equals(var))
                return distinctObjects;

            return 0;
        }
    }
}
