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

  public void generateFileChunks(JspWriter out, HttpServletRequest req) 
    throws IOException {
    long startOffset = 0;
    
    int chunkSizeToView = 0;

    String referrer = req.getParameter("referrer");
    boolean noLink = false;
    if (referrer == null) {
      noLink = true;
    }

    String filename = req.getParameter("filename");
    if (filename == null) {
      out.print("Invalid input (filename absent)");
      return;
    }

    String chunkSizeToViewStr = req.getParameter("chunkSizeToView");
    if (chunkSizeToViewStr != null && Integer.parseInt(chunkSizeToViewStr) > 0)
      chunkSizeToView = Integer.parseInt(chunkSizeToViewStr);
    else chunkSizeToView = jspHelper.defaultChunkSizeToView;

    if (!noLink)
      out.print("<h2><a href=\"" + referrer + "\">" + filename + "</a></h2>");
    else
      out.print("<h2>" + filename + "</h2>");
    out.print("<b>Chunk Size to view (in bytes, upto file's DFS blocksize): </b>");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value=" +
              chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\"><hr>");
    out.print("<input type=\"hidden\" name=\"filename\" value=\"" + filename +
              "\">");
    if (!noLink)
      out.print("<input type=\"hidden\" name=\"referrer\" value=\"" + 
                referrer+ "\">");

    //fetch the block from the datanode that has the last block for this file
    DFSClient dfs = new DFSClient(jspHelper.nameNodeAddr, 
                                         jspHelper.conf);
    LocatedBlock blocks[] = dfs.namenode.open(filename);
    if (blocks == null || blocks.length == 0) {
      out.print("No datanodes contain blocks of file "+filename);
      dfs.close();
      return;
    }
    LocatedBlock lastBlk = blocks[blocks.length - 1];
    long blockSize = lastBlk.getBlock().getNumBytes();
    long blockId = lastBlk.getBlock().getBlockId();
    DatanodeInfo chosenNode;
    try {
      chosenNode = jspHelper.bestNode(lastBlk);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }      
    InetSocketAddress addr = DataNode.createSocketAddr(chosenNode.getName());
    //view the last chunkSizeToView bytes while Tailing
    if (blockSize >= chunkSizeToView)
      startOffset = blockSize - chunkSizeToView;
    else startOffset = 0;

    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" READONLY>");
    jspHelper.streamBlockInAscii(addr, blockId, blockSize, startOffset, chunkSizeToView, out);
    out.print("</textarea>");
    dfs.close();
  }

%>



<html>
<meta http-equiv="refresh" content=60>
<title>Hadoop DFS File Viewer</title>

<body>
<form action="/tail.jsp" method=GET>
<% 
   generateFileChunks(out,request);
%>
</form>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
