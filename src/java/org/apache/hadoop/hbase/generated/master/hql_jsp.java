package org.apache.hadoop.hbase.generated.master;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.util.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hql.TableFormatter;
import org.apache.hadoop.hbase.hql.ReturnMsg;
import org.apache.hadoop.hbase.hql.generated.HQLParser;
import org.apache.hadoop.hbase.hql.Command;
import org.apache.hadoop.hbase.hql.formatter.HtmlTableFormatter;

public final class hql_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

  private static java.util.Vector _jspx_dependants;

  public java.util.List getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    JspFactory _jspxFactory = null;
    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;


    try {
      _jspxFactory = JspFactory.getDefaultFactory();
      response.setContentType("text/html;charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;

      out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \n  \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"> \n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\"/>\n<title>HQL</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hbase.css\" />\n</head>\n\n<body>\n<a id=\"logo\" href=\"http://wiki.apache.org/lucene-hadoop/Hbase\"><img src=\"/static/hbase_logo_med.gif\" alt=\"Hbase Logo\" title=\"Hbase Logo\" /></a>\n<h1 id=\"page_title\"><a href=\"http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell\">HQL</a></h1>\n<p id=\"links_menu\"><a href=\"/master.jsp\">Home</a></p>\n<hr id=\"head_rule\" />\n");
 String query = request.getParameter("q");
   if (query == null) {
     query = "";
   }

      out.write("\n<form action=\"/hql.jsp\" method=\"get\">\n    <p>\n    <label for=\"query\">Query: </label>\n    <input type=\"text\" name=\"q\" id=\"q\" size=\"60\" value=\"");
      out.print( query );
      out.write("\" />\n    <input type=\"submit\" value=\"submit\" />\n    </p>\n </form>\n <p>Enter 'help;' -- thats 'help' plus a semi-colon -- for the list of <em>HQL</em> commands.\n Data Definition, SHELL, INSERTS, DELETES, and UPDATE commands are disabled in this interface\n </p>\n \n ");

  if (query.length() > 0) {
 
      out.write("\n <hr/>\n ");

    HQLParser parser = new HQLParser(query, out, new HtmlTableFormatter(out));
    Command cmd = parser.terminatedCommand();
    if (cmd.getCommandType() != Command.CommandType.SELECT) {
 
      out.write("\n  <p>");
      out.print( cmd.getCommandType() );
      out.write("-type commands are disabled in this interface.</p>\n ");

    } else { 
      ReturnMsg rm = cmd.execute(new HBaseConfiguration());
      String summary = rm == null? "": rm.toString();
 
      out.write("\n  <p>");
      out.print( summary );
      out.write("</p>\n ");
 } 
  }
 
      out.write("\n</body>\n</html>\n");
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      if (_jspxFactory != null) _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
