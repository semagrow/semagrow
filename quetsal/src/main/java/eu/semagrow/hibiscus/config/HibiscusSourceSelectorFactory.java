package eu.semagrow.hibiscus.config;

import eu.semagrow.core.source.SourceSelector;
import eu.semagrow.hibiscus.selector.HibiscusSourceSelector;


/**
 * Created by angel on 15/6/2015.
 */
public class HibiscusSourceSelectorFactory extends QuetsalSourceSelectorFactory
{

    public static String SRCSELECTOR_TYPE = "aksw:hibiscus";

    public String getType() { return SRCSELECTOR_TYPE; }

    public SourceSelector getSourceSelectorInternal() { return new HibiscusSourceSelector(); }

}
