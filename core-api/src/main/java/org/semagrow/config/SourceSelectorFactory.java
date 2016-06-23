package org.semagrow.config;

import org.semagrow.selector.SourceSelector;

/**
 *
 * @author Angelos Charalambidis
 */
public interface SourceSelectorFactory {

    SourceSelectorImplConfig getConfig();

    SourceSelector getSourceSelector(SourceSelectorImplConfig config) throws SourceSelectorConfigException;

    String getType();
}
