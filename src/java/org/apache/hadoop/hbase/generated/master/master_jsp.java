package org.apache.hadoop.hbase.generated.master;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.hbase.HMaster;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMaster.MetaRegion;
import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hql.ShowCommand;
import org.apache.hadoop.hbase.hql.TableFormatter;
import org.apache.hadoop.hbase.hql.ReturnMsg;
import org.apache.hadoop.hbase.hql.formatter.HtmlTableFormatter;
import org.apache.hadoop.hbase.HTableDescriptor;

public final class master_jsp extends org.apache.jasper.runtime.HttpJspBase
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


  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  HBaseConfiguration conf = master.getConfiguration();
  TableFormatter formatter = new HtmlTableFormatter(out);
  ShowCommand show = new ShowCommand(out, formatter, "tables");
  HServerAddress rootLocation = master.getRootRegionLocation();
  Map<Text, MetaRegion> onlineRegions = master.getOnlineMetaRegions();
  Map<String, HServerInfo> serverToServerInfos =
    master.getServersToServerInfo();
  int interval = conf.getInt("hbase.regionserver.msginterval", 6000)/1000;

      out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \n  \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"> \n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\"/>\n<title>Hbase Master: ");
      out.print( master.getMasterAddress());
      out.write("</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hbase.css\" />\n</head>\n\n<body>\n\n<a id=\"logo\" href=\"http://wiki.apache.org/lucene-hadoop/Hbase\"><img src=\"/static/hbase_logo_med.gif\" alt=\"Hbase Logo\" title=\"Hbase Logo\" /></a>\n<h1 id=\"page_title\">Master: ");
      out.print(master.getMasterAddress());
      out.write("</h1>\n<p id=\"links_menu\"><a href=\"/hql.jsp\">HQL</a>, <a href=\"/logs/\">Local logs</a>, <a href=\"/stacks\">Thread Dump</a>, <a href=\"/logLevel\">Log Level</a></p>\n<hr id=\"head_rule\" />\n\n<h2>Master Attributes</h2>\n<table>\n<tr><th>Attribute Name</th><th>Value</th><th>Description</th></tr>\n<tr><td>Version</td><td>");
      out.print( VersionInfo.getVersion() );
      out.write(',');
      out.write(' ');
      out.write('r');
      out.print( VersionInfo.getRevision() );
      out.write("</td><td>Hbase version and svn revision</td></tr>\n<tr><td>Compiled</td><td>");
      out.print( VersionInfo.getDate() );
      out.write(',');
      out.write(' ');
      out.print( VersionInfo.getUser() );
      out.write("</td><td>When this version was compiled and by whom</td></tr>\n<tr><td>Filesystem</td><td>");
      out.print( conf.get("fs.default.name") );
      out.write("</td><td>Filesystem hbase is running on</td></tr>\n<tr><td>Hbase Root Directory</td><td>");
      out.print( master.getRootDir().toString() );
      out.write("</td><td>Location of hbase home directory</td></tr>\n</table>\n\n<h2>Online META Regions</h2>\n");
 if (rootLocation != null) { 
      out.write("\n<table>\n<tr><th>Name</th><th>Server</th></tr>\n<tr><td>");
      out.print( HConstants.ROOT_TABLE_NAME.toString() );
      out.write("</td><td>");
      out.print( rootLocation.toString() );
      out.write("</td></tr>\n");

  if (onlineRegions != null && onlineRegions.size() > 0) { 
      out.write('\n');
      out.write(' ');
      out.write(' ');
 for (Map.Entry<Text, HMaster.MetaRegion> e: onlineRegions.entrySet()) {
    MetaRegion meta = e.getValue();
  
      out.write("\n  <tr><td>");
      out.print( meta.getRegionName().toString() );
      out.write("</td><td>");
      out.print( meta.getServer().toString() );
      out.write("</td></tr>\n  ");
 }
  } 
      out.write("\n</table>\n");
 } 
      out.write("\n\n<h2>Tables</h2>\n");
 ReturnMsg msg = show.execute(conf); 
      out.write("\n<p>");
      out.print(msg );
      out.write("</p>\n\n<h2>Region Servers</h2>\n");
 if (serverToServerInfos != null && serverToServerInfos.size() > 0) { 
      out.write('\n');
 int totalRegions = 0;
   int totalRequests = 0; 

      out.write("\n\n<table>\n<tr><th rowspan=");
      out.print( serverToServerInfos.size() + 1);
      out.write("></th><th>Address</th><th>Start Code</th><th>Load</th></tr>\n\n");
   for (Map.Entry<String, HServerInfo> e: serverToServerInfos.entrySet()) {
       HServerInfo hsi = e.getValue();
       String url = "http://" +
         hsi.getServerAddress().getBindAddress().toString() + ":" +
         hsi.getInfoPort() + "/";
       String load = hsi.getLoad().toString();
       totalRegions += hsi.getLoad().getNumberOfRegions();
       totalRequests += hsi.getLoad().getNumberOfRequests();
       long startCode = hsi.getStartCode();
       String address = hsi.getServerAddress().toString();

      out.write("\n<tr><td><a href=\"");
      out.print( url );
      out.write('"');
      out.write('>');
      out.print( address );
      out.write("</a></td><td>");
      out.print( startCode );
      out.write("</td><td>");
      out.print( load );
      out.write("</td></tr>\n");
   } 
      out.write("\n<tr><th>Total: </th><td>servers: ");
      out.print( serverToServerInfos.size() );
      out.write("</td><td>&nbsp;</td><td>requests: ");
      out.print( totalRequests );
      out.write(" regions: ");
      out.print( totalRegions );
      out.write("</td></tr>\n</table>\n\n<p>Load is requests per <em>hbase.regionsserver.msginterval</em> (");
      out.print(interval);
      out.write(" second(s)) and count of regions loaded</p>\n");
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
