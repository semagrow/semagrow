package eu.semagrow.core.impl.evalit;

import eu.semagrow.core.evalit.SessionId;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.util.UUID;

/**
* Created by angel on 6/23/14.
*/
public class SessionUUID implements SessionId {

    private UUID id;

    SessionUUID(UUID realId) { this.id = realId; }

    public static SessionUUID createUniqueId() { return new SessionUUID(UUID.randomUUID()); }

    @Override
    public int hashCode() { return id.hashCode(); }

    @Override
    public boolean equals(Object id2) {
        if (id2 instanceof SessionUUID)
            return id.equals(((SessionUUID)id2).id);

        return false;
    }

    @Override
    public String toString() { return id.toString(); }


    public IRI toURI() { return SimpleValueFactory.getInstance().createIRI("urn:"+toString()); }
}
