<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.dfs.*"
  import="java.text.DateFormat"
%>
<%!
  FSNamesystem fsn = FSNamesystem.getFSNamesystem();
  String namenodeLabel = fsn.getDFSNameNodeMachine() + ":" + fsn.getDFSNameNodePort();
  long currentTime;
  JspHelper jspHelper = new JspHelper();

  public void generateLiveNodeData(JspWriter out, DatanodeInfo d) 
    throws IOException {
    long c = d.getCapacity();
    long r = d.getRemaining();
    long u = c - r;
    String cGb = DFSShell.limitDecimal((1.0 * c)/(1024*1024*1024), 2);
    String uGb = DFSShell.limitDecimal((1.0 * u)/(1024*1024*1024), 2);
    String percentUsed;
    if (c > 0) 
      percentUsed = DFSShell.limitDecimal(((1.0 * u)/c)*100, 2);
    else
      percentUsed = "100"; 
    out.print("<td style=\"vertical-align: top;\"> <b>" + 
              d.getName() +
              "</b>&nbsp;<br><i><b>LastContact:</b>&nbsp;" + 
             (currentTime - d.getLastUpdate())/1000 + " second(s) back;&nbsp;");
    out.print("<b>Total raw bytes:</b>&nbsp;" + c + "(" + cGb + 
              "&nbsp;GB);&nbsp;");
    out.print("<b>Percent used:</b>&nbsp;" + percentUsed);
    out.print("</i></td>");
  }

  public void generateDFSHealthReport(JspWriter out) throws IOException {
    Vector live = new Vector();
    Vector dead = new Vector();
    jspHelper.DFSNodesStatus(live, dead);
    if (live.isEmpty() && dead.isEmpty()) {
      out.print("There are no datanodes in the cluster");
    }
    else {
      out.print("<br><b>Number of live data stores: " + live.size() + 
                ", dead datanodes: " + dead.size() + "</b></br>");
      out.print("<table style=\"width: 100%; text-align: left;\" border=\"1\""+
                " cellpadding=\"2\" cellspacing=\"2\">");
      out.print("<tbody>");
      out.print("<tr>");
      out.print("<td style=\"vertical-align: top;\"><B>Live Data Stores</B><br></td>");
      out.print("<td style=\"vertical-align: top;\"><B>Dead Data Stores</B><br></td>");
      out.print("</tr>");
      int i = 0;
      int min = (live.size() > dead.size()) ? dead.size() : live.size();
      int max = (live.size() > dead.size()) ? live.size() : dead.size();
      currentTime = System.currentTimeMillis();
      for (i = 0; i < min; i++) {
        DatanodeInfo l = (DatanodeInfo)live.elementAt(i);
        DatanodeInfo d = (DatanodeInfo)dead.elementAt(i);
        out.print("<tr>");
        generateLiveNodeData(out, l);
        out.print("<td style=\"vertical-align: top;\">" + 
                  d.getName() +
                  "<br></td>");
        out.print("</tr>");
      }
      int type = (live.size() > dead.size()) ? 1 : 0;
      for (i = min; i < max; i++) {
        out.print("<tr>");
        if (type == 1) {
          DatanodeInfo l = (DatanodeInfo)live.elementAt(i);
          generateLiveNodeData(out, l);
          out.print("<td style=\"vertical-align: top;\"><br></td>");
        }
        else if (type == 0) {
          DatanodeInfo d = (DatanodeInfo)dead.elementAt(i);
          out.print("<td style=\"vertical-align: top;\"><br></td>");
          out.print("<td style=\"vertical-align: top;\">" + 
                    d.getName() +
                    "<br></td>");
        }
        out.print("</tr>");
      }
      out.print("</tbody></table>");
    }
  }
  public String totalCapacity() {
    return fsn.totalCapacity() + "(" + DFSShell.limitDecimal((1.0 * fsn.totalCapacity())/(1024*1024*1024), 2) + " GB)";
  }
  public String totalRemaining() {
    return fsn.totalRemaining() + "(" +  DFSShell.limitDecimal(fsn.totalRemaining()/(1024*1024*1024), 2) + " GB)";
  }
%>

<html>

<title>Hadoop NameNode <%=namenodeLabel%></title>

<body>
<h1>NameNode '<%=namenodeLabel%>'</h1>

This NameNode has been up since <%= fsn.getStartTime()%>.<br><br>
<b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b>
<hr>
<h2>Cluster Summary</h2>
<b> <%= jspHelper.getSafeModeText()%> </b>
<p>
The capacity of this cluster is <%= totalCapacity()%> and remaining is <%= totalRemaining()%>.
<br>
<% 
   generateDFSHealthReport(out); 
%>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
