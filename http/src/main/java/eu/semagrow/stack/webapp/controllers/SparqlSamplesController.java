package eu.semagrow.stack.webapp.controllers;

import eu.semagrow.commons.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletResponse;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
@Controller
@RequestMapping("/samples")
public class SparqlSamplesController {
     
    public static final String ATTR_SAMPLES_RESULT = "samples";
    public static final String TEMPLATE_SAMPLES_RESULT = "sparqlsamples";

    private HashMap<Resource, HashMap<URI,HashSet<Value>>> samplesData = null;
    
    String sparqlSamplesFileName = "sparql.samples.ttl";
    File sparqlSamplesFile = null;
    
    
    public SparqlSamplesController() {
        try {
            sparqlSamplesFile = FileUtils.getFile(sparqlSamplesFileName);
        } catch(Exception ioex){}
    }
    
    @PostConstruct
    public void startUp() throws FileNotFoundException, IOException, RDFParseException, RDFHandlerException  {
        if(this.sparqlSamplesFile!=null){
            HashMapHandler handler = new HashMapHandler();
            RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            parser.setRDFHandler(handler);
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(this.sparqlSamplesFile);
                parser.parse(fis, "");
                this.samplesData = handler.getSamplesData();
            } finally {
                if(fis!=null){
                    fis.close();
                }
            }            
        }
    }
    
    @PreDestroy
    public void shutDown(){
    }
            
    @RequestMapping(method=RequestMethod.GET)
    public ModelAndView getSparqlEndpointForm(HttpServletResponse response) throws IOException{
        ModelAndView mav = new ModelAndView(TEMPLATE_SAMPLES_RESULT);
                     mav.addObject(ATTR_SAMPLES_RESULT, this.samplesData);       
        return mav;
    }
    
    @RequestMapping(value="/reload", method=RequestMethod.GET)
    public void reload(HttpServletResponse response) throws IOException, FileNotFoundException, RDFParseException, RDFHandlerException{        
            sparqlSamplesFile = FileUtils.getFile(sparqlSamplesFileName);
            this.startUp();
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
    }
    
    private class HashMapHandler extends RDFHandlerBase {
        private HashMap<Resource, HashMap<URI,HashSet<Value>>> samplesData = new HashMap<>();

        public HashMap<Resource, HashMap<URI, HashSet<Value>>> getSamplesData() {
            return samplesData;
        }
        
        @Override
        public void handleStatement(Statement st) throws RDFHandlerException {            
            if(!this.samplesData.containsKey(st.getSubject())){
                this.samplesData.put(st.getSubject(), new HashMap<URI,HashSet<Value>>());
            }
            
            if(!this.samplesData.get(st.getSubject()).containsKey(st.getPredicate())){
                this.samplesData.get(st.getSubject()).put(st.getPredicate(), new HashSet<Value>());
            }
            
            this.samplesData.get(st.getSubject()).get(st.getPredicate()).add(st.getObject());            
        }        
    }
}
