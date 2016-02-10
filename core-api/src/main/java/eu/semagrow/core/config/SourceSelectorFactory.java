package eu.semagrow.core.config;

import eu.semagrow.core.source.SourceSelector;

/**
 *
 * @author Angelos Charalambidis
 */
public interface SourceSelectorFactory {

    SourceSelectorImplConfig getConfig();

    SourceSelector getSourceSelector(SourceSelectorImplConfig config) throws SourceSelectorConfigException;

    String getType();
}
