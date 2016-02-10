package eu.semagrow.core.impl.planner;

import org.openrdf.model.URI;

/**
 * @author Angelos Charalambidis
 */
public class Site {

    public static Site LOCAL = new Site();

    private URI endpointURI;

    public Site(URI uri) { this.endpointURI = uri; }

    public Site() { this.endpointURI = null; }

    public boolean isLocal() {
        return endpointURI == null;
    }

    public boolean isRemote() {
        return !isLocal();
    }

    public URI getURI() { return endpointURI; }

    public boolean equals(Object o) {
        if (o instanceof Site) {
            Site s = (Site)o;
            return (s.isLocal() && this.isLocal()) ||
                    (s.getURI() != null && s.getURI().equals(this.getURI()));
        }
        return false;
    }
}
