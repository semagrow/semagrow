package eu.semagrow.core.impl.plan.ops;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import java.util.*;

/**
 * Created by angel on 6/9/14.
 */
public class ProvenanceValue implements Value {

    private List<URI> provenances;

    private final String separator = ";";

    public ProvenanceValue(URI provenance) {
        provenances = new LinkedList<URI>();
        provenances.add(provenance);
    }

    public ProvenanceValue(Collection<URI> provenances) {

        this.provenances = new LinkedList<URI>(provenances);
    }

    public ProvenanceValue(ProvenanceValue v) {
        this.provenances = new LinkedList<URI>(v.provenances);
    }

    public String stringValue() {
        Set<URI> uniqueSet = new HashSet<URI>(this.provenances);
        StringBuilder sb = new StringBuilder();
        Iterator<URI> iter = uniqueSet.iterator();
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
