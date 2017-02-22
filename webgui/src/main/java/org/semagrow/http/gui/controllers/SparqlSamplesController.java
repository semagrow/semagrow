package org.semagrow.http.gui.controllers;

import org.semagrow.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
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

    private HashMap<Resource, HashMap<IRI,HashSet<Value>>> samplesData = null;
    
    String sparqlSamplesFileName = "sparql.samples.ttl";
    File sparqlSamplesFile = null;
    
    
    public SparqlSamplesController() {
        try {
            sparqlSamplesFile = FileUtils.getFile(sparqlSamplesFileName);
        } catch(Exception ioex){}
    }
    
    @PostConstruct
    public void startUp() throws IOException, RDFParseException, RDFHandlerException  {
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
    public void reload(HttpServletResponse response) throws IOException, RDFParseException, RDFHandlerException{
            sparqlSamplesFile = FileUtils.getFile(sparqlSamplesFileName);
            this.startUp();
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
    }
    
    private class HashMapHandler extends AbstractRDFHandler {
        private HashMap<Resource, HashMap<IRI,HashSet<Value>>> samplesData = new HashMap<>();

        public HashMap<Resource, HashMap<IRI, HashSet<Value>>> getSamplesData() {
            return samplesData;
        }
        
        @Override
        public void handleStatement(Statement st) throws RDFHandlerException {            
            if(!this.samplesData.containsKey(st.getSubject())){
                this.samplesData.put(st.getSubject(), new HashMap<IRI,HashSet<Value>>());
            }
            
            if(!this.samplesData.get(st.getSubject()).containsKey(st.getPredicate())){
                this.samplesData.get(st.getSubject()).put(st.getPredicate(), new HashSet<Value>());
            }
            
            this.samplesData.get(st.getSubject()).get(st.getPredicate()).add(st.getObject());            
        }        
    }
}
