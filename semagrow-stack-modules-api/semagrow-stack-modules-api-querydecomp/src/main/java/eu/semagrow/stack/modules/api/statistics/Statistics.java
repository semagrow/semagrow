package eu.semagrow.stack.modules.api.statistics;

/**
 * Created by angel on 7/5/2015.
 */
public interface Statistics {

    long getCardinality();

    long getVarCardinality(String var);

}
