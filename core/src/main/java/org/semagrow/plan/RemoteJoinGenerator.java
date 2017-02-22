package org.semagrow.plan;

import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;
import org.eclipse.rdf4j.query.algebra.Join;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A {@link JoinImplGenerator} that pushes the {@Join} to the site if
 * the capabilities of the site allows joining and if the site of the plans
 * of the two operands is the same.
 * @author acharal
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
