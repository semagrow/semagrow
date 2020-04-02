package org.semagrow.selector;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.semagrow.plan.Pair;
import org.semagrow.plan.util.BPGCollector;

import java.util.*;

public class PrefixQueryAwareSourceSelector extends SourceSelectorWrapper implements QueryAwareSourceSelector {

    private PrefixBase prefixBase = new PrefixBase();
    private Map<StatementPattern,Collection<SourceMetadata>> selectorMap = new HashMap<>();
    private boolean processed = false;

    public PrefixQueryAwareSourceSelector(SourceSelector selector) {
        super(selector);
    }

    public void setMetadataRepository(Repository metadata) {
        prefixBase.setMetadata(metadata);
    }

    @Override
    public void processTupleExpr(TupleExpr expr) {
        if (getWrappedSelector() instanceof QueryAwareSourceSelector) {
            ((QueryAwareSourceSelector) getWrappedSelector()).processTupleExpr(expr);
        }
        Collection<TupleExpr> bpgs = BPGCollector.process(expr);
        for (TupleExpr bpg: bpgs) {
            processBPG(bpg);
        }
        processed = true;
    }

    private void processBPG(TupleExpr expr) {

        Map<StatementPattern,Map<SourceMetadata,Map<String,Collection<String>>>> phatMap = new HashMap<>();

        for (StatementPattern pattern:  StatementPatternCollector.process(expr)) {
            Map<SourceMetadata,Map<String,Collection<String>>> sourceMap = new HashMap<>();

            for (SourceMetadata source: getWrappedSelector().getSources(pattern, null, EmptyBindingSet.getInstance())) {
                Map<String,Collection<String>> varMap = new HashMap<>();

                if (!(pattern.getSubjectVar().hasValue())) {
                    Collection<String> prefixes = prefixBase.getSubjectRegexPattern(pattern, endpointOfSource(source));
                    varMap.put(pattern.getSubjectVar().getName(), prefixes);
                }
                if (!(pattern.getObjectVar().hasValue())) {
                    Collection<String> prefixes = prefixBase.getObjectRegexPattern(pattern, endpointOfSource(source));
                    varMap.put(pattern.getObjectVar().getName(), prefixes);
                }
                sourceMap.put(source,varMap);
            }
            phatMap.put(pattern, sourceMap);
        }

        List<Pair<StatementPattern,SourceMetadata>> toRemove;

        do {
            toRemove = new ArrayList<>();

            for (StatementPattern p1 : phatMap.keySet()) {
                for (StatementPattern p2 : phatMap.keySet()) {
                    if (!(p1.equals(p2))) {
                        Set<Var> vars = new HashSet<>(p1.getVarList());
                        vars.retainAll(p2.getVarList());
                        vars.removeIf(var -> var.hasValue());

                        for (Var var : vars) {
                            for (SourceMetadata s1 : phatMap.get(p1).keySet()) {
                                boolean delete = true;
                                for (SourceMetadata s2 : phatMap.get(p2).keySet()) {
                                    Collection<String> prf1 = phatMap.get(p1).get(s1).get(var.getName());
                                    Collection<String> prf2 = phatMap.get(p2).get(s2).get(var.getName());

                                    // FIXME: assume that prefixes are disjoint

                                    Set<String> commonPrf = new HashSet<>(prf1);
                                    commonPrf.retainAll(prf2);

                                    if (!(commonPrf.isEmpty())) {
                                        delete = false;
                                    }
                                }
                                if (delete) {
                                    toRemove.add(new Pair<>(p1,s1));
                                }
                            }
                        }
                    }
                }

                for (Pair pair: toRemove) {
                    phatMap.get(pair.getFirst()).remove(pair.getSecond());
                }
            }
        } while (!(toRemove.isEmpty()));

        for (StatementPattern pattern: phatMap.keySet()) {
            if (selectorMap.containsKey(pattern)) {
                selectorMap.get(pattern).addAll(phatMap.get(pattern).keySet());
            }
            else {
                Set<SourceMetadata> sources = new HashSet<>();
                sources.addAll(phatMap.get(pattern).keySet());
                selectorMap.put(pattern, sources);
            }
        }
    }

    private Resource endpointOfSource(SourceMetadata source) {
        return source.getSites().iterator().next().getID();
    }

    @Override
    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings)
    {
        if (processed) {
            return selectorMap.get(pattern);
        }
        return super.getWrappedSelector().getSources(pattern, dataset, bindings);
    }

    private Collection<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        Collection<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for(StatementPattern p : patterns) {
            list.addAll(this.getSources(p, dataset, bindings));
        }
        return list;
    }

    @Override
    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings)  {
        if(expr instanceof StatementPattern) {
            return getSources((StatementPattern)expr, dataset, bindings);
        }

        Collection<StatementPattern> patterns  = StatementPatternCollector.process(expr);
        return getSources(patterns, dataset, bindings);
    }

}
