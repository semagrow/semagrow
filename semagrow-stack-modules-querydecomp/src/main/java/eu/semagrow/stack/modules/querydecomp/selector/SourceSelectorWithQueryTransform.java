package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.api.source.SourceMetadata;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import eu.semagrow.stack.modules.api.transformation.EquivalentURI;
import eu.semagrow.stack.modules.api.transformation.QueryTransformation;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import java.util.*;

/**
 * Created by angel on 11/17/14.
 */
public class SourceSelectorWithQueryTransform extends SourceSelectorWrapper {

    private QueryTransformation queryTransformation;

    public SourceSelectorWithQueryTransform(SourceSelector selector,
                                            QueryTransformation queryTransformation) {
        super(selector);
        this.queryTransformation = queryTransformation;
    }

    @Override
    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {

        List<SourceMetadata> lst = super.getSources(pattern, dataset, bindings);

        Collection<FuzzyEntry<StatementPattern>> transformed = transformPattern(pattern);

        for (FuzzyEntry<StatementPattern> fep : transformed) {
            List<SourceMetadata> lt = super.getSources(fep.getElem(), dataset, bindings);
            for (SourceMetadata m : lt) {
                lst.add(new FuzzySourceMetadata(pattern, m, fep));
            }
        }

        return lst;
    }

    private Collection<FuzzyEntry<StatementPattern>> transformPattern(StatementPattern pattern)
    {
        Set<FuzzyEntry<StatementPattern>> transformedPatterns = new HashSet<FuzzyEntry<StatementPattern>>();

        //queryTransformation.retrieveEquivalentURIs(pattern)
        Var sVar = pattern.getSubjectVar();
        Var pVar = pattern.getPredicateVar();
        Var oVar = pattern.getObjectVar();

        double patternProximity = 1.0;

        for (FuzzyEntry<Var> sVar1 : transformVar(sVar)) {
            for (FuzzyEntry<Var> pVar1 : transformVar(pVar)) {
                for (FuzzyEntry<Var> oVar1 : transformVar(oVar)) {
                    StatementPattern p = new StatementPattern(sVar1.getElem(), pVar1.getElem(), oVar1.getElem());
                    if (!p.equals(pattern)) {

                        double prox = Math.min(Math.min(sVar1.getProximity(), pVar1.getProximity()), oVar1.getProximity());
                        transformedPatterns.add(new FuzzyEntry<StatementPattern>(p, prox));
                    }
                }
            }
        }

        return transformedPatterns;
    }

    private Collection<FuzzyEntry<Var>> transformVar(Var v) {
        Set<FuzzyEntry<Var>> vars = new HashSet<FuzzyEntry<Var>>();

        Value val = v.getValue();
        if (val != null && val instanceof URI) {
            Collection<EquivalentURI> uris =  getEquivalentURI(v);
            for(EquivalentURI uri : uris)
            {
                Var v1 = transformVar(v,uri);
                vars.add(new FuzzyEntry<Var>(v1, uri.getProximity()));
            }
        }
        else {
            vars.add(new FuzzyEntry<Var>(v));
        }
        return vars;
    }

    private Collection<EquivalentURI> getEquivalentURI(Var v) {
        Value val = v.getValue();

        if (val != null && val instanceof URI) {
            URI uri = (URI)val;
            return queryTransformation.retrieveEquivalentURIs(uri);
        }

        return Collections.emptySet();
    }

    private Var transformVar(Var v, EquivalentURI uri) {
        if (!v.hasValue())
            return v;
        else {
            Var v1 = new Var("const-" + uri.getTargetURI().toString(), uri.getTargetURI());
            v1.setConstant(true);
            return v1;
        }
    }

    private class FuzzyEntry<T> {
        private T elem;
        private double proximity;
        public FuzzyEntry(T e) { this(e,1.0); }
        public FuzzyEntry(T e, double p) { elem = e; proximity = p ;}

        public T getElem() { return elem; }
        public double getProximity() { return proximity; }
    }

    private class FuzzySourceMetadata implements SourceMetadata {

        private FuzzyEntry<StatementPattern> entry;
        private StatementPattern original;
        private SourceMetadata metadata;

        public FuzzySourceMetadata(StatementPattern original,
                                   SourceMetadata metadata,
                                   FuzzyEntry<StatementPattern> entry)
        {
            this.entry = entry;
            this.original = original;
            this.metadata = metadata;
        }


        public List<URI> getEndpoints() { return metadata.getEndpoints(); }

        public StatementPattern original() { return original; }

        public StatementPattern target() { return entry.getElem(); }

        public Collection<URI> getSchema(String var) { return metadata.getSchema(var); }

        public boolean isTransformed() { return true; }

        public double getSemanticProximity() { return entry.getProximity(); }
    }
}
