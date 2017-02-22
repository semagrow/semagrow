package org.semagrow.http.gui.controllers;

import org.semagrow.art.CsvCreator;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
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
    
    @Autowired
    CsvCreator creator;
    
    @RequestMapping(value="/welcome", method=RequestMethod.GET)
    public ModelAndView welcome(HttpServletResponse response) throws IOException{
        ModelAndView mav = new ModelAndView("welcome");
        return mav;
    }
    
    @RequestMapping(value="/page", method=RequestMethod.GET, params={ "template" })
    public ModelAndView page(HttpServletResponse response, @RequestParam(value="template") String template) throws IOException{
        ModelAndView mav = new ModelAndView(template);
        return mav;
    }

    @RequestMapping(value="/monitoring", method=RequestMethod.GET)
    public void monitor(HttpServletResponse response) throws IOException{

        if(this.creator!=null){
            response.setContentType("text/csv");
            //CsvCreator creator = new CsvCreator("jdbc:postgresql://127.0.0.1:5432/logging", "postgres", "postgres");
            for (String line : creator.getCsv()) {
                response.getWriter().println(line);
            }
            response.getWriter().flush();        
        } else {
            response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED);
        }
    }
}
