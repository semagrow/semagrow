package eu.semagrow.hibiscus.config;

import eu.semagrow.core.config.SourceSelectorConfigException;
import eu.semagrow.core.config.SourceSelectorFactory;
import eu.semagrow.core.config.SourceSelectorImplConfig;
import eu.semagrow.core.source.SourceSelector;
import eu.semagrow.hibiscus.util.SummariesGenerator;
import org.aksw.simba.quetzal.configuration.QuetzalConfig;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.BasicParserSettings;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by angel on 26/6/2015.
 */
public abstract class QuetsalSourceSelectorFactory implements SourceSelectorFactory {


    public SourceSelectorImplConfig getConfig()
    {
        return new QuetsalSourceSelectorImplConfig(getType());
    }

    public abstract String getType();

    protected void init(QuetsalSourceSelectorImplConfig config)
            throws SourceSelectorConfigException
    {
        String summaries = config.getSummariesFile();

        if (summaries == null) {
            try {
                //cleanUp();
                summaries = generateSummaries(config.getMetadataFile());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RDFParseException e) {
                e.printStackTrace();
            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }

        String mode = config.getMode();
        double commonPredThreshold = config.getCommonPredicateThreshold();

        try {
            QuetzalConfig.initialize(summaries, mode, commonPredThreshold);
        } catch (Exception e) {
            throw new SourceSelectorConfigException(e);
        }
    }

    public SourceSelector getSourceSelector(SourceSelectorImplConfig config)
            throws SourceSelectorConfigException
    {
        if (config instanceof QuetsalSourceSelectorImplConfig)
        {
            init((QuetsalSourceSelectorImplConfig)config);
            return getSourceSelectorInternal();

        } else {
            throw new SourceSelectorConfigException("SourceSelectorImplConfig is not of appropriate instance");
        }
    }

    protected abstract SourceSelector getSourceSelectorInternal();


    private String generateSummaries(String VoidPath) throws IOException, RDFParseException, RDFHandlerException {

        //String fname = "/tmp/summaries.n3";
        File f = File.createTempFile(VoidPath + "-quetsal", ".n3");

        RDFFormat writerFormat = RDFWriterRegistry.getInstance().getFileFormatForFileName(f.getAbsolutePath());
        RDFWriterFactory writerFactory = RDFWriterRegistry.getInstance().get(writerFormat);

        RDFWriter writer = writerFactory.getWriter(new FileOutputStream(f));

        SummariesGenerator sg = new SummariesGenerator(writer);

        RDFFormat format = Rio.getParserFormatForFileName(VoidPath);
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.setRDFHandler(sg);
        parser.parse(new FileInputStream(VoidPath), "");

        return f.getAbsolutePath();
    }

    private void cleanUp() {
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString() + "/summaries\\/memorystore.data";
        Path memstorepath = Paths.get(s);
        try {
            Files.delete(memstorepath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
