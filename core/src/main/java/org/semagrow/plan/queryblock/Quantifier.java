package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

import java.util.*;

/**
 * Quantifier represents the way that a {@link QueryBlock} can range.
 * {@link Quantifier.Quantification::ALL} and {@link Quantifier.Quantification::ANY} are provisioned
 * for nested queries.
 * @author acharal
 */
public class Quantifier {

    public enum Quantification {
        EACH,
        ALL,
        ANY
    }

    public class Var extends AbstractQueryModelNode implements ValueExpr {

        private String name;

        private Quantifier quantifier;

        private EquivalentClass equivVars;

        Var(String name, Quantifier quantifier) {
            this.name = name;
            this.quantifier = quantifier;
            this.equivVars = new EquivalentClass(name, this);
        }

        public String getName() { return name; };

        public Quantifier getQuantifier() { return quantifier; }

        public Collection<Var> getEquivVars() { return equivVars.getVars(); }

        public void setEquivWith(Var var) {
            this.getEquivClass().merge(var.getEquivClass());
        }

        EquivalentClass getEquivClass() { return equivVars; }

        void setEquivClass(EquivalentClass clazz) { this.equivVars = clazz; }

        @Override
        public <X extends Exception> void visit(QueryModelVisitor<X> queryModelVisitor) throws X {
            queryModelVisitor.meetOther(this);
        }

        public Var clone() { return (Var)super.clone(); }

        public String toString() { return getName(); }

        @Override
        public int hashCode() { return name.hashCode() + quantifier.hashCode(); }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Quantifier.Var) {
                Quantifier.Var v = (Quantifier.Var)o;
                return this.getQuantifier().equals(v.getQuantifier()) && this.getName().equals(v.getName());
            }
            return false;
        }

        private class EquivalentClass {

            private String clazzName;

            private Collection<Var> vars;

            public EquivalentClass(String name, Var...vars) {
                this.clazzName = name;
                this.vars = Arrays.asList(vars);
            }

            public String getName() { return clazzName; }

            public Collection<Var> getVars() { return vars; }

            public void merge(EquivalentClass clazz) {
                this.vars.addAll(clazz.getVars());

                for (Var v : clazz.getVars())
                    v.setEquivClass(this);
            }
        }
    }

    private Quantification quantification;

    private QueryBlock block;

    private QueryBlock parent;

    private Collection<Var> vars;

    public Quantifier(QueryBlock b) {
        setBlock(b);
        setQuantification(Quantification.EACH);
    }

    public Quantifier(QueryBlock b, Quantification q) {
        this(b);
        setQuantification(q);
    }

    public Collection<Var> getVariables() { return vars; }

    public Optional<Var> getVariable(String name) {
        return vars.stream().filter(v -> v.getName().equals(name)).findFirst();
    }

    public void setQuantification(Quantification q) { this.quantification = q; }

    public QueryBlock getParent() { return parent; }

    public void setParent(QueryBlock parent) { this.parent = parent; }

    private void setBlock(QueryBlock b) {
        this.block = b;

        Collection<String> blockVars = this.block.getOutputVariables();

        vars = new ArrayList<>(blockVars.size());

        for (String var : blockVars)
            vars.add(new Var(var, this));
    }

    public QueryBlock getBlock()  { return block; }

    public Quantification getQuantification() { return quantification; }

    public boolean isFrom() { return getQuantification() == Quantification.EACH; }

    public Collection<Quantifier> dependsOn() {
        DependencyVisitor visitor = new DependencyVisitor();
        block.visit(visitor);
        return visitor.external;
    }

    private class DependencyVisitor extends AbstractQueryBlockVisitor<RuntimeException> {

        Collection<Quantifier> external = new HashSet<>();

        public void meet(SelectBlock block) {
            super.meet(block);

            external.removeAll(block.getQuantifiers());

            Collection<Predicate> predicates = block.getPredicates();
            for (Predicate p : predicates) {
                Collection<Quantifier> qc = new HashSet<>(p.getEEL());
                qc.removeAll(block.getQuantifiers());
                external.addAll(qc);
            }
        }

    }

}
