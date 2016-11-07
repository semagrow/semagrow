package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.semagrow.local.LocalSite;
import org.semagrow.plan.*;
import org.semagrow.selector.SourceMetadata;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author acharal
 */
public class PatternBlock extends AbstractQueryBlock {

    private StatementPattern pattern;

    public PatternBlock(StatementPattern pattern) {
        assert pattern != null;
        this.pattern = pattern;
    }

    public Set<String> getOutputVariables() {
        return VarNameCollector.process(pattern);
    }

    public Collection<Plan> getPlans(CompilerContext context) {

        Collection<SourceMetadata> metadata = context.getSourceSelector().getSources(pattern, null, EmptyBindingSet.getInstance());

        return metadata.stream()
                .map(m ->  m.getSites().stream().map(s -> {
                    PlanProperties props = new PlanProperties();
                    props.setSite(s);
                    return context.asPlan(m.target(), props);
                }))
                .reduce((Stream<Plan> r, Stream<Plan> s) ->  {
                            Collection<Plan> collect = s.collect(Collectors.toList());
                            return r.flatMap(m -> collect.stream().flatMap(n -> union(context, m, n).stream()));
                        }).orElse(Stream.empty())
                .collect(Collectors.toList());
    }

    private Collection<Plan> union(CompilerContext context, Plan p1, Plan p2)
    {
        RequestedPlanProperties props = new RequestedPlanProperties();

        props.setSite(LocalSite.getInstance());

        Collection<Plan> pc1 = context.enforceProps(p1, props);
        Collection<Plan> pc2 = context.enforceProps(p2, props);

        return pc1.stream()
                .flatMap( s1 -> pc2.stream().map( s2 ->
                            context.asPlan(new Union(s1, s2))
                )).collect(Collectors.toList());
    }

    public boolean hasDuplicates() { return true; }

}
