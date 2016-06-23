package org.semagrow.plan;

import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;
import org.eclipse.rdf4j.query.algebra.Join;

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
            p1.getProperties().getSite().equals(p2.getProperties().getSite()))
        {
            Site s = p1.getProperties().getSite();
            SiteCapabilities cap = s.getCapabilities();

            if (cap.isJoinable(p1, p2)) {
                Join j = new Join(p1, p2);
                l.add(j);
            }
        }

        return l;
    }

}
