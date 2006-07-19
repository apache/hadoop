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

    String filename = req.getParameter("filename");
    if (filename == null) {
      out.print("Invalid input (filename absent)");
      return;
    }
    
    String blockIdStr = null;
    long blockId = 0;
    blockIdStr = req.getParameter("blockId");
    if (blockIdStr == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }
    blockId = Long.parseLong(blockIdStr);

    String blockSizeStr;
    long blockSize = 0;
    blockSizeStr = req.getParameter("blockSize"); 
    if (blockSizeStr == null) {
      out.print("Invalid input (blockSize absent)");
      return;
    }
    blockSize = Long.parseLong(blockSizeStr);
    
    String chunkSizeToViewStr = req.getParameter("chunkSizeToView");
    if (chunkSizeToViewStr != null && Integer.parseInt(chunkSizeToViewStr) > 0)
      chunkSizeToView = Integer.parseInt(chunkSizeToViewStr);
    else chunkSizeToView = jspHelper.defaultChunkSizeToView;

    String startOffsetStr;
    startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else startOffset = Long.parseLong(startOffsetStr);
   
    out.print("<h2>File: " + filename + "</h2>");
    out.print("<a href=\"http://" + req.getServerName() + ":" + 
              req.getServerPort() + "/browseData.jsp?filename=" + filename + 
              "\">Go back to File details</a><br>");
    out.print("<b>Chunk Size to view (in bytes, upto file's DFS blocksize): </b>");
    out.print("<input type=\"hidden\" name=\"blockId\" value=\"" + blockId + 
              "\">");
    out.print("<input type=\"hidden\" name=\"blockSize\" value=\"" + 
              blockSize + "\">");
    out.print("<input type=\"hidden\" name=\"startOffset\" value=\"" + 
              startOffset + "\">");
    out.print("<input type=\"hidden\" name=\"filename\" value=\"" + filename +
              "\">");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value=" +
              chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\"><hr>");

    //Determine the prev & next blocks
    DFSClient dfs = null;
    long nextStartOffset = 0;
    long nextBlockSize = 0;
    String nextBlockIdStr = null;
    String nextHost = req.getServerName();
    //determine data for the next link
    if (startOffset + chunkSizeToView >= blockSize) {
      //we have to go to the next block from this point onwards
      dfs = new DFSClient(jspHelper.nameNodeAddr, jspHelper.conf);
      LocatedBlock[] blocks = dfs.namenode.open(filename);
      for (int i = 0; i < blocks.length; i++) {
        if (blocks[i].getBlock().getBlockId() == blockId) {
          if (i != blocks.length - 1) {
            LocatedBlock nextBlock = blocks[i+1];
            nextBlockIdStr = Long.toString(nextBlock.getBlock().getBlockId());
            nextStartOffset = 0;
            nextBlockSize = nextBlock.getBlock().getNumBytes();
            String host = jspHelper.bestNode(nextBlock).getHost();
            nextHost = InetAddress.getByName(host).getCanonicalHostName();
          }
        }
      }
    } 
    else {
      //we are in the same block
      nextBlockIdStr = blockIdStr;
      nextStartOffset = startOffset + chunkSizeToView;
      nextBlockSize = blockSize;
    }
    String nextUrl = null;
    if (nextBlockIdStr != null) {
      nextUrl = "http://" + nextHost + ":" + 
                jspHelper.datanode.getDataNodeInfoPort() + 
                "/browseBlock.jsp?blockId=" + nextBlockIdStr +
                "&blockSize=" + nextBlockSize + "&startOffset=" + 
                nextStartOffset + "&filename=" + filename +
                "&chunkSizeToView=" + chunkSizeToView;
      out.print("<a href=\"" + nextUrl + "\">Next</a>&nbsp;&nbsp;");        
    }
    //determine data for the prev link
    String prevBlockIdStr = null;
    long prevStartOffset = 0;
    long prevBlockSize = 0;
    String prevHost = req.getServerName();
    if (startOffset == 0) {
      if (dfs == null) 
        dfs = new DFSClient(jspHelper.nameNodeAddr, jspHelper.conf);
      LocatedBlock [] blocks = dfs.namenode.open(filename);
      for (int i = 0; i < blocks.length; i++) {
        if (blocks[i].getBlock().getBlockId() == blockId) {
          if (i != 0) {
            LocatedBlock prevBlock = blocks[i-1];
            prevBlockIdStr = Long.toString(prevBlock.getBlock().getBlockId());
            prevStartOffset = prevBlock.getBlock().getNumBytes() - chunkSizeToView;
            if (prevStartOffset < 0)
              prevStartOffset = 0;
            prevBlockSize = prevBlock.getBlock().getNumBytes();
            String host = jspHelper.bestNode(prevBlock).getHost();
            prevHost = InetAddress.getByName(host).getCanonicalHostName();
          }
        }
      }
    }
    else {
      //we are in the same block
      prevBlockIdStr = blockIdStr;
      prevStartOffset = startOffset - chunkSizeToView;
      if (prevStartOffset < 0) prevStartOffset = 0;
      prevBlockSize = blockSize;
    }

    String prevUrl = null;
    if (prevBlockIdStr != null) {
      prevUrl = "http://" + prevHost + ":" + 
                jspHelper.datanode.getDataNodeInfoPort() + 
                "/browseBlock.jsp?blockId=" + prevBlockIdStr + 
                "&blockSize=" + prevBlockSize + "&startOffset=" + 
                prevStartOffset + "&filename=" + filename + 
                "&chunkSizeToView=" + chunkSizeToView;
      out.print("<a href=\"" + prevUrl + "\">Prev</a>&nbsp;&nbsp;");
    }
    out.print("<hr>");

    jspHelper.streamBlockInAscii(jspHelper.dataNodeAddr, blockId, 
                                 blockSize, startOffset, chunkSizeToView, out);
    //dfs.close();
  }

%>



<html>

<title>Hadoop DFS File Viewer</title>

<body>
<form action="/browseBlock.jsp" method=GET>
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
