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

  int rowNum = 0;
  int colNum = 0;

  String rowTxt() { colNum = 0;
      return "<tr class=\"" + (((rowNum++)%2 == 0)? "rowNormal" : "rowAlt")
          + "\"> "; }
  String colTxt() { return "<td id=\"col" + ++colNum + "\"> "; }
  void counterReset () { colNum = 0; rowNum = 0 ; }

  long diskBytes = 1024 * 1024 * 1024;
  String diskByteStr = "GB";

  String sorterField = null;
  String sorterOrder = null;

  String NodeHeaderStr(String name) {
      String ret = "class=header";
      String order = "ASC";
      if ( name.equals( sorterField ) ) {
          ret += sorterOrder;
          if ( sorterOrder.equals("ASC") )
              order = "DSC";
      }
      ret += " onClick=\"window.document.location=" +
          "'/dfshealth.jsp?sorter/field=" + name + "&sorter/order=" +
          order + "'\" title=\"sort on this column\"";
      
      return ret;
  }
      
  public void generateLiveNodeData( JspWriter out, DatanodeDescriptor d,
                                    String suffix, boolean alive )
    throws IOException {
    
    String name = d.getName();
    if ( !name.matches( "\\d+\\.\\d+.\\d+\\.\\d+.*" ) ) 
        name = name.replaceAll( "\\.[^.:]*", "" );
    
    int idx = (suffix != null && name.endsWith( suffix )) ?
        name.indexOf( suffix ) : -1;    
    out.print( rowTxt() + "<td class=\"name\"><a title=\"" + d.getName() +
               "\">" + (( idx > 0 ) ? name.substring(0, idx) : name) +
               (( alive ) ? "" : "\n") );
    if ( !alive )
        return;
    
    long c = d.getCapacity();
    long r = d.getRemaining();
    long u = c - r;
    
    String percentUsed;
    if (c > 0) 
      percentUsed = DFSShell.limitDecimal(((1.0 * u)/c)*100, 2);
    else
      percentUsed = "100";
    
    out.print("<td class=\"lastcontact\"> " +
              ((currentTime - d.getLastUpdate())/1000) +
	      "<td class=\"size\">" +
              DFSShell.limitDecimal(c*1.0/diskBytes, 2) +
	      "<td class=\"pcused\">" + percentUsed +
              "<td class=\"blocks\">" + d.numBlocks() + "\n");
  }

  public void generateDFSHealthReport(JspWriter out,
                                      HttpServletRequest request)
                                      throws IOException {
    ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    jspHelper.DFSNodesStatus(live, dead);

    sorterField = request.getParameter("sorter/field");
    sorterOrder = request.getParameter("sorter/order");
    if ( sorterField == null )
        sorterField = "name";
    if ( sorterOrder == null )
        sorterOrder = "ASC";

    jspHelper.sortNodeList(live, sorterField, sorterOrder);
    jspHelper.sortNodeList(dead, "name", "ASC");
    
    // Find out common suffix. Should this be before or after the sort?
    String port_suffix = null;
    if ( live.size() > 0 ) {
        String name = live.get(0).getName();
        int idx = name.indexOf(':');
        if ( idx > 0 ) {
            port_suffix = name.substring( idx );
        }
        
        for ( int i=1; port_suffix != null && i < live.size(); i++ ) {
            if ( live.get(i).getName().endsWith( port_suffix ) == false ) {
                port_suffix = null;
                break;
            }
        }
    }
        
    counterReset();
    
    out.print( "<div id=\"dfstable\"> <table>\n" +
	       rowTxt() + colTxt() + "Capacity" + colTxt() + ":" + colTxt() +
	       DFSShell.byteDesc( fsn.totalCapacity() ) +
	       rowTxt() + colTxt() + "Remaining" + colTxt() + ":" + colTxt() +
	       DFSShell.byteDesc( fsn.totalRemaining() ) +
	       rowTxt() + colTxt() + "Used" + colTxt() + ":" + colTxt() +
	       DFSShell.limitDecimal((fsn.totalCapacity() -
				      fsn.totalRemaining())*100.0/
				     (fsn.totalCapacity() + 1e-10), 2) + " %" +
	       rowTxt() + colTxt() +
               "<a href=\"#LiveNodes\">Live Nodes</a> " +
               colTxt() + ":" + colTxt() + live.size() +
	       rowTxt() + colTxt() +
               "<a href=\"#DeadNodes\">Dead Nodes</a> " +
               colTxt() + ":" + colTxt() + dead.size() +
               "</table></div><br><hr>\n" );
    
    if (live.isEmpty() && dead.isEmpty()) {
	out.print("There are no datanodes in the cluster");
    }
    else {
        
        currentTime = System.currentTimeMillis();
	out.print( "<div id=\"dfsnodetable\"> "+
                   "<a name=\"LiveNodes\" id=\"title\">" +
                   "Live Datanodes: " + live.size() + "</a>" +
                   "<br><br>\n<table border=1 cellspacing=0>\n" );

        counterReset();
        
	if ( live.size() > 0 ) {
            
            if ( live.get(0).getCapacity() > 1024 * diskBytes ) {
                diskBytes *= 1024;
                diskByteStr = "TB";
            }

	    out.print( "<tr class=\"headerRow\"> <th " +
                       NodeHeaderStr("name") + "> Node <th " +
                       NodeHeaderStr("lastcontact") + "> Last Contact <th " +
                       NodeHeaderStr("size") + "> Size (" + diskByteStr +
                       ") <th " + NodeHeaderStr("pcused") +
                       "> Used (%) <th " + NodeHeaderStr("blocks") +
                       "> Blocks\n" );
            
	    for ( int i=0; i < live.size(); i++ ) {
		generateLiveNodeData( out, live.get(i), port_suffix, true );
	    }
	}
        out.print("</table>\n");

        counterReset();
	
	out.print("<br> <a name=\"DeadNodes\" id=\"title\"> " +
                  " Dead Datanodes : " +dead.size() + "</a><br><br>\n");

	if ( dead.size() > 0 ) {
	    out.print( "<table border=1 cellspacing=0> <tr id=\"row1\"> " +
		       "<td> Node \n" );
	    
	    for ( int i=0; i < dead.size() ; i++ ) {
                generateLiveNodeData( out, dead.get(i), port_suffix, false );
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
