package eu.semagrow.hibiscus.config;

import eu.semagrow.core.source.SourceSelector;
import eu.semagrow.hibiscus.selector.TBSSSourceSelector;

/**
 * Created by angel on 24/6/2015.
 */
public class TBSSSourceSelectorFactory extends QuetsalSourceSelectorFactory
{

    public static String SRCSELECTOR_TYPE = "aksw:tbss";

    public String getType() { return SRCSELECTOR_TYPE; }

    public SourceSelector getSourceSelectorInternal() { return new TBSSSourceSelector(); }

}
