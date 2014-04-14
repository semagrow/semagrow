package eu.semagrow.modules.rioutils.queryresultio;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.QueryResultFormat;
import org.openrdf.query.resultio.QueryResultWriterBase;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriter;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class HTMLTableWriter extends QueryResultWriterBase implements TupleQueryResultWriter {
    
    public static TupleQueryResultFormat HTML_TABLE = new TupleQueryResultFormat(
            "TEXT/HTML", 
            "text/html", 
            Charset.forName("UTF-8"), 
            "html");
    
    private final Writer writer;
    private List<String> bindingsNames;
    
    public HTMLTableWriter(OutputStream out) {
        Writer w = new OutputStreamWriter(out, Charset.forName("UTF-8"));
               writer = new BufferedWriter(w, 1024);
    }
    
    public QueryResultFormat getQueryResultFormat() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void handleNamespace(String string, String string1) throws QueryResultHandlerException {
        //ignored
    }

    public void startDocument() throws QueryResultHandlerException {
        //ignores
    }

    public void handleStylesheet(String string) throws QueryResultHandlerException {
        System.out.println("handleStylesheet");
    }

    public void startHeader() throws QueryResultHandlerException {
        System.out.println("startHeader");
    }

    public void endHeader() throws QueryResultHandlerException {
        System.out.println("endHeader");
    }

    public void handleBoolean(boolean bln) throws QueryResultHandlerException {
        System.out.println("handleBoolean");
    }

    public void handleLinks(List<String> list) throws QueryResultHandlerException {
        System.out.println("handleLinks " + list);
    }

    public void startQueryResult(List<String> bindings) throws TupleQueryResultHandlerException {
        this.bindingsNames = bindings;
        try {
            writer.append("<html><head><title></title></head><body>");
            writer.append("<table><tr>");
            for(String binding : bindings){
                writer.append("<th>");
                writer.append(binding);
                writer.append("</th>");
            }
            writer.append("</tr>");
        } catch (IOException ex) {
            throw new TupleQueryResultHandlerException(ex);
        }
    }

    public void endQueryResult() throws TupleQueryResultHandlerException {
        try {            
            writer.append("</body></html>");
            writer.flush();
        } catch (IOException e) {
            throw new TupleQueryResultHandlerException(e);
        }        
    }

    public void handleSolution(BindingSet bs) throws TupleQueryResultHandlerException {
        try {
            writer.append("<tr>");
            for(String binding : this.bindingsNames){
                writer.append("<td>");
                writer.append(bs.getValue(binding).stringValue());
                writer.append("</td>");            
            }
            writer.append("</tr>");
        } catch (IOException ex) {
            throw new TupleQueryResultHandlerException(ex);
        }        
    }

    public TupleQueryResultFormat getTupleQueryResultFormat() {
        return HTML_TABLE;
    }



}
