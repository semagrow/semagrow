package eu.semagrow.core.impl.plan;

import eu.semagrow.core.plan.*;
import eu.semagrow.core.source.Site;
import eu.semagrow.core.source.SourceCapabilities;
import org.openrdf.query.algebra.Join;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by angel on 31/3/2016.
 */
class RemoteJoinGenerator implements JoinImplGenerator {

    @Override
    public Collection<Join> generate(Plan p1, Plan p2, PlanGenerationContext ctx) {
        Collection<Join> l = new LinkedList<Join>();

        if (p1.getProperties().getSite().isRemote() &&
            p2.getProperties().getSite().isRemote() &&
            p1.getProperties().getSite().getID().equals(p2.getProperties().getSite().getID()))
        {
            Site s = p1.getProperties().getSite();
            SourceCapabilities cap = s.getCapabilities();

            if (cap.isJoinable(p1, p2)) {
                Join j = new Join(p1, p2);
                l.add(j);
            }
        }

        return l;
    }

}
