<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.dfs.*"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
  import="java.lang.Math"    
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
    
    String percentUsed;
    if (c > 0) 
      percentUsed = DFSShell.limitDecimal(((1.0 * u)/c)*100, 2);
    else
      percentUsed = "100";
    
    out.print("<tr> <td id=\"col1\">" + d.getName() +
              "<td>" + ((currentTime - d.getLastUpdate())/1000) +
	      "<td>" + DFSShell.byteDesc(c) +
	      "<td>" + percentUsed + "\n");
  }

  public void generateDFSHealthReport(JspWriter out,
                                      HttpServletRequest request)
                                      throws IOException {
    Vector live = new Vector();
    Vector dead = new Vector();
    jspHelper.DFSNodesStatus(live, dead);
    
    out.print( "<div id=\"dfstable\"> <table>\n" +
	       "<tr> <td id=\"col1\"> Capacity <td> : <td>" +
	       DFSShell.byteDesc( fsn.totalCapacity() ) +
	       "<tr> <td id=\"col1\"> Remaining <td> : <td>" +
	       DFSShell.byteDesc( fsn.totalRemaining() ) +
	       "<tr> <td id=\"col1\"> Used <td> : <td>" +
	       DFSShell.limitDecimal((fsn.totalCapacity() -
				      fsn.totalRemaining())*100.0/
				     (fsn.totalCapacity() + 1e-10), 2) +
	       "%<tr> <td id=\"col1\"> Live Nodes <td> : <td>" + live.size() +
	       "<tr> <td id=\"col1\"> Dead Nodes <td> : <td>" + dead.size() +
               "</table></div><br><hr>\n" );
    
    if (live.isEmpty() && dead.isEmpty()) {
	out.print("There are no datanodes in the cluster");
    }
    else {
	
        currentTime = System.currentTimeMillis();
	out.print( "<div id=\"dfsnodetable\"> "+
                   "<a id=\"title\">" +
                   "Live Datanodes: " + live.size() + "</a>" +
                   "<br><br>\n<table border=\"1\">\n" );

	if ( live.size() > 0 ) {

	    out.print( "<tr id=\"row1\">" +
		       "<td> Node <td> Last Contact <td> Size " +
		       "<td> Used (%)\n" );
            
	    for ( int i=0; i < live.size(); i++ ) {
		DatanodeInfo d = ( DatanodeInfo ) live.elementAt(i);
		generateLiveNodeData( out, d );
	    }
	}
        out.print("</table>\n");
	
	out.print("<br> <a id=\"title\"> " +
                  " Dead Datanodes : " +dead.size() + "</a><br><br>\n");

	if ( dead.size() > 0 ) {
	    out.print( "<table border=\"1\"> <tr id=\"row1\"> " +
		       "<td> Node \n" );
	    
	    for ( int i=0; i < dead.size() ; i++ ) {
		DatanodeInfo d = ( DatanodeInfo ) dead.elementAt(i);
		out.print( "<tr> <td> " + d.getName() + "\n" );
	    }
	    
	    out.print("</table>\n");
	}
	out.print("</div>");
    }
  }
%>

<html>

<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop NameNode <%=namenodeLabel%></title>
    
<body>
<h1>NameNode '<%=namenodeLabel%>'</h1>


<div id="dfstable"> <table>	  
<tr> <td id="col1"> Started: <td> <%= fsn.getStartTime()%>
<tr> <td id="col1"> Version: <td> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%>
<tr> <td id="col1"> Compiled: <td> <%= VersionInfo.getDate()%> by
                 <%= VersionInfo.getUser()%>
</table></div><br>				      

<b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b>
<hr>
<h3>Cluster Summary</h3>
<b> <%= jspHelper.getSafeModeText()%> </b>

<% 
    generateDFSHealthReport(out, request); 
%>
<hr>

<h3>Local logs</h3>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
