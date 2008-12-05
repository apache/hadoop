package org.apache.hadoop.chukwa.hicc;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Iframe  extends HttpServlet {
	
    private String id;
    private String height="100%";

	public void doGet(HttpServletRequest request,
	           HttpServletResponse response) throws IOException, ServletException {
		if(request.getParameter("boxId")!=null) {
			this.id=request.getParameter("boxId");
		} else {
			this.id="0";
		}
		response.setHeader("boxId", request.getParameter("boxId"));
	    PrintWriter out = response.getWriter();
	    StringBuffer source = new StringBuffer();
	    String requestURL = request.getRequestURL().toString().replaceFirst("iframe/", "");
	    source.append(requestURL);	    
	    source.append("?");
	    Enumeration names = request.getParameterNames();
	    while(names.hasMoreElements()) {
	    	String key = (String) names.nextElement();
	    	String[] values = request.getParameterValues(key);
	    	for(int i=0;i<values.length;i++) {
	    	    source.append(key+"="+values[i]+"&");
	    	}
	    	if(key.toLowerCase().intern()=="height".intern()) {
	    		height = request.getParameter(key);
	    	}
	    }
        out.println("<html><body><iframe id=\"iframe"+ this.id +"\" "+
             "src=\"" + source + "\" width=\"100%\" height=\"" + height + "\" "+
             "frameborder=\"0\" style=\"overflow: hidden\"></iframe>");
	}
   public void doPost(HttpServletRequest request,
           HttpServletResponse response) throws IOException, ServletException {
	   doGet(request, response);
   }
}
