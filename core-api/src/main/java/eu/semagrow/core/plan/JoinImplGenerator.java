package eu.semagrow.core.plan;

import org.eclipse.rdf4j.query.algebra.Join;

import java.util.Collection;

/**
 * Created by angel on 31/3/2016.
 */
public interface JoinImplGenerator {

    Collection<Join> generate(Plan p1, Plan p2, PlanGenerationContext ctx);

}
