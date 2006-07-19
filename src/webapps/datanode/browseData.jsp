<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.net.*"
  import="org.apache.hadoop.dfs.*"
  import="org.apache.hadoop.io.*"
  import="org.apache.hadoop.conf.*"
  import="java.text.DateFormat"
%>
<%!
  static JspHelper jspHelper = new JspHelper();

  public void generateFileDetails(JspWriter out, HttpServletRequest req) 
    throws IOException {

    String filename = req.getParameter("filename");
    if (filename == null || filename.length() == 0) {
      out.print("Invalid input");
      return;
    }

    String blockSizeStr = req.getParameter("blockSize"); 
    long blockSize;
    if (blockSizeStr != null && blockSizeStr.length() != 0)
      blockSize = Long.parseLong(blockSizeStr);

    DFSClient dfs = new DFSClient(jspHelper.nameNodeAddr, jspHelper.conf);
    LocatedBlock[] blocks = dfs.namenode.open(filename);
    out.print("<h2>Filename: "+filename+"</h2>");
    out.print("<a href=\"http://" + req.getServerName() + ":" + 
              req.getServerPort() + 
              "/browseDirectory.jsp?dir=" + new File(filename).getParent() + 
              "\"><i>Go back to dir listing</i></a><br><hr>");
    //Add the various links for looking at the file contents
    //URL for downloading the full file
    String fqdn = InetAddress.getByName(jspHelper.datanode.getDataNodeMachine()).getCanonicalHostName();
    String downloadUrl = "http://" + fqdn + 
                         ":" + jspHelper.datanode.getDataNodeInfoPort() + 
                         "/streamFile?" + "filename=" + filename;
    out.print("<a href=\"" + downloadUrl + "\">Download this file</a><br>");
    
    DatanodeInfo chosenNode;
    //URL for TAIL 
    LocatedBlock lastBlk = blocks[blocks.length - 1];
    long blockId = lastBlk.getBlock().getBlockId();
    blockSize = lastBlk.getBlock().getNumBytes();
    try {
      chosenNode = jspHelper.bestNode(lastBlk);
    } catch (IOException e) {
      out.print(e.toString());
      //dfs.close();
      return;
    }
    fqdn = InetAddress.getByName(chosenNode.getHost()).getCanonicalHostName();
    String tailUrl = "http://" + fqdn + ":" +
                     jspHelper.datanode.getDataNodeInfoPort() + 
                     "/tail.jsp?filename=" + filename;
    out.print("<a href=\"" + tailUrl + "\">TAIL this file</a><br>");

    //URL for chunk viewing of a file
    LocatedBlock firstBlk = blocks[0];
    blockId = firstBlk.getBlock().getBlockId();
    blockSize = firstBlk.getBlock().getNumBytes();
    try {
      chosenNode = jspHelper.bestNode(firstBlk);
    } catch (IOException e) {
      out.print(e.toString());
      //dfs.close();
      return;
    }
    fqdn = InetAddress.getByName(chosenNode.getHost()).getCanonicalHostName();
    String chunkViewUrl = "http://" + fqdn + ":" +
                     jspHelper.datanode.getDataNodeInfoPort() + 
                     "/browseBlock.jsp?blockId=" + Long.toString(blockId) +
                     "&tail=false&blockSize=" + blockSize +
                     "&filename=" + filename;
    out.print("<a href=\"" + chunkViewUrl + 
              "\">View this file (in a chunked fashion)</a><br>");
    out.print("<hr>"); 
    out.print("<B>Total number of blocks: "+blocks.length+"</B><HR>");
    //generate a table and dump the info
    String [] headings = new String[2];
    headings[0] = "Block ID"; headings[1] = "Datanodes containing this block"; 
    jspHelper.addTableHeader(out);
    jspHelper.addTableRow(out, headings);
    String cols [] = new String[2];
    for (int i = 0; i < blocks.length; i++) {
      blockId = blocks[i].getBlock().getBlockId();
      blockSize = blocks[i].getBlock().getNumBytes();
      cols[0] = "blk_" + Long.toString(blockId);
      DatanodeInfo[] locs = blocks[i].getLocations();
      String locations = new String();
      for (int j = 0; j < locs.length; j++) {
        fqdn = InetAddress.getByName(locs[j].getHost()).getCanonicalHostName();
        String blockUrl = "http://"+ fqdn + ":" +
                          jspHelper.dataNodeInfoPort +
                          "/browseBlock.jsp?blockId=" + Long.toString(blockId) +
                          "&blockSize=" + blockSize +
                          "&filename=" + filename;
        locations += "<a href=\"" + blockUrl + "\">" + fqdn + "</a>";
        if (j < locs.length - 1)
          locations += ", ";
      }
      cols[1] = locations;
      jspHelper.addTableRow(out, cols);
    }
    jspHelper.addTableFooter(out);
    //dfs.close();
  }

%>

<html>

<title>Hadoop DFS File Browsing</title>

<body>

<% 
   generateFileDetails(out,request);
%>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
