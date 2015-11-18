package eu.semagrow.stack.webapp.controllers;

import eu.semagrow.commons.CONSTANTS;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
@Controller
@RequestMapping("/")
public class IndexController {
    
    @RequestMapping(value="/welcome", method=RequestMethod.GET)
    public ModelAndView welcome(HttpServletResponse response) throws IOException{
        ModelAndView mav = new ModelAndView("welcome");
        return mav;
    }
    
    @RequestMapping(value="/page", method=RequestMethod.GET, params={ CONSTANTS.WEBAPP.PARAM_TEMPLATE })
    public ModelAndView page(HttpServletResponse response, @RequestParam(value="template") String template) throws IOException{
        ModelAndView mav = new ModelAndView(template);
        return mav;
    }

    @RequestMapping(value="/monitoring", method=RequestMethod.GET)
    public void monitor(HttpServletResponse response) throws IOException{

        response.setContentType("text/csv");

        response.getWriter().println("\"Query\";\"QueryString\";\"Endpoint\";\"Time\"");
        response.getWriter().println("\"Q1\";\"SELECT\";\"Total\";\"1000\"");
        response.getWriter().println("\"Q1\";\"SELECT\";\"Decomposition\";\"70\"");
        response.getWriter().println("\"Q1\";\"SELECT\";\"EP2\";\"100\"");
        response.getWriter().println("\"Q1\";\"SELECT\";\"EP1\";\"120\"");
        response.getWriter().println("\"Q2\";\"SELECT\";\"Total\";\"800\"");
        response.getWriter().println("\"Q2\";\"SELECT\";\"EP1\";\"80\"");
        response.getWriter().println("\"Q2\";\"SELECT\";\"EP2\";\"70\"");
        response.getWriter().println("\"Q2\";\"SELECT\";\"Decomposition\";\"90\"");
        response.getWriter().println("\"Q3\";\"SELECT\";\"Total\";\"1400\"");
        response.getWriter().println("\"Q3\";\"SELECT\";\"EP1\";\"300\"");
        response.getWriter().println("\"Q3\";\"SELECT\";\"Decomposition\";\"30\"");
        response.getWriter().println("\"Q3\";\"SELECT\";\"EP2\";\"410\"");
        response.getWriter().println("\"Q4\";\"SELECT\";\"Total\";\"1200\"");
        response.getWriter().println("\"Q4\";\"SELECT\";\"EP1\";\"500\"");
        response.getWriter().println("\"Q4\";\"SELECT\";\"EP2\";\"810\"");
        response.getWriter().println("\"Q4\";\"SELECT\";\"Decomposition\";\"90\"");
        response.getWriter().println("\"Q5\";\"SELECT * WHERE { ?s ?p ?o \\n FILTER(regex(str(?o),\"\"xxx\"\",'i')) }\";\"EP1\";\"700\"");
        response.getWriter().println("\"Q5\";\"SELECT * WHERE { ?s ?p ?o FILTER(regex(str(?o),\"\"xxx\"\",'i')) }\";\"EP2\";\"510\"");
        response.getWriter().println("\"Q5\";\"SELECT * WHERE { ?s ?p ?o FILTER(regex(str(?o),\"\"xxx\"\",'i')) }\";\"Decomposition\";\"70\"");
        response.getWriter().println("\"Q5\";\"SELECT * WHERE { ?s ?p ?o FILTER(regex(str(?o),\"\"xxx\"\",'i')) }\";\"Total\";\"1600\"");
        response.getWriter().println("\"Q6\";\"SELECT\";\"EP1\";\"300\"");
        response.getWriter().println("\"Q6\";\"SELECT\";\"EP2\";\"410\"");
        response.getWriter().println("\"Q6\";\"SELECT\";\"Decomposition\";\"100\"");
        response.getWriter().println("\"Q6\";\"SELECT\";\"Total\";\"1300\"");

        response.getWriter().flush();
    }
}
