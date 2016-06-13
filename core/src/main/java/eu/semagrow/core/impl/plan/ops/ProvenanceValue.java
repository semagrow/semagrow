package eu.semagrow.core.impl.plan.ops;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

import java.util.*;

/**
 * Created by angel on 6/9/14.
 */
public class ProvenanceValue implements Value {

    private List<IRI> provenances;

    private final String separator = ";";

    public ProvenanceValue(IRI provenance) {
        provenances = new LinkedList<IRI>();
        provenances.add(provenance);
    }

    public ProvenanceValue(Collection<IRI> provenances) {

        this.provenances = new LinkedList<IRI>(provenances);
    }

    public ProvenanceValue(ProvenanceValue v) {
        this.provenances = new LinkedList<IRI>(v.provenances);
    }

    public String stringValue() {
        Set<IRI> uniqueSet = new HashSet<IRI>(this.provenances);
        StringBuilder sb = new StringBuilder();
        Iterator<IRI> iter = uniqueSet.iterator();
        if (iter.hasNext()) {
            sb.append(iter.next().stringValue());
            while (iter.hasNext()) {
                sb.append(separator).append(iter.next().stringValue());
            }
        }
        return sb.toString();
    }

    public void merge(ProvenanceValue pv) {
        provenances.addAll(pv.provenances);
    }

    public String toString() { return stringValue(); }

}
