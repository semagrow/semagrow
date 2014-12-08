package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.api.evaluation.SessionId;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;

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


    public URI toURI() { return ValueFactoryImpl.getInstance().createURI("urn:"+toString()); }
}
