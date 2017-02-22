package org.semagrow.http.views;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.springframework.web.servlet.View;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * Created by angel on 17/6/2016.
 */
public class ExplainView implements View {

    static public View getInstance() { return new ExplainView(); }

    static final public String HEADERS_ONLY = "headersOnly";
    static final public String DECOMPOSED = "decomposed";

    @Override
    public String getContentType() { return null; }

    @Override
    public void render(Map<String, ?> map, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws Exception {
        boolean headersOnly = (Boolean)map.get(HEADERS_ONLY);
        if (!headersOnly) {
            ServletOutputStream out = httpServletResponse.getOutputStream();

            TupleExpr e = (TupleExpr) map.get(DECOMPOSED);
            String serialized = e.toString();
            out.print(serialized);
            out.close();
        }
    }

}
