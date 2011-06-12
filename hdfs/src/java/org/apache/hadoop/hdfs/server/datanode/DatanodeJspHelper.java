/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public class DatanodeJspHelper {
  private static final DataNode datanode = DataNode.getDataNode();

  private static DFSClient getDFSClient(final UserGroupInformation user,
                                        final InetSocketAddress addr,
                                        final Configuration conf
                                        ) throws IOException,
                                                 InterruptedException {
    return
      user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        public DFSClient run() throws IOException {
          return new DFSClient(addr, conf);
        }
      });
  }

  /**
   * Get the default chunk size.
   * @param conf the configuration
   * @return the number of bytes to chunk in
   */
  private static int getDefaultChunkSize(Configuration conf) {
    return conf.getInt("dfs.default.chunk.view.size", 32 * 1024);
  }

  static void generateDirectoryStructure(JspWriter out, 
                                         HttpServletRequest req,
                                         HttpServletResponse resp,
                                         Configuration conf
                                         ) throws IOException,
                                                  InterruptedException {
    final String dir = JspHelper.validatePath(req.getParameter("dir"));
    if (dir == null) {
      out.print("Invalid input");
      return;
    }
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    DFSClient dfs = getDFSClient(ugi, datanode.getNameNodeAddr(), conf);
    String target = dir;
    final HdfsFileStatus targetStatus = dfs.getFileInfo(target);
    if (targetStatus == null) { // not exists
      out.print("<h3>File or directory : " + target + " does not exist</h3>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, target);
    } else {
      if (!targetStatus.isDir()) { // a file
        List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(dir, 0, 1)
            .getLocatedBlocks();

        LocatedBlock firstBlock = null;
        DatanodeInfo[] locations = null;
        if (blocks.size() > 0) {
          firstBlock = blocks.get(0);
          locations = firstBlock.getLocations();
        }
        if (locations == null || locations.length == 0) {
          out.print("Empty file");
        } else {
          DatanodeInfo chosenNode = JspHelper.bestNode(firstBlock);
          String fqdn = InetAddress.getByName(chosenNode.getHost())
              .getCanonicalHostName();
          String datanodeAddr = chosenNode.getName();
          int datanodePort = Integer.parseInt(datanodeAddr.substring(
              datanodeAddr.indexOf(':') + 1, datanodeAddr.length()));
          String redirectLocation = "http://" + fqdn + ":"
              + chosenNode.getInfoPort() + "/browseBlock.jsp?blockId="
              + firstBlock.getBlock().getBlockId() + "&blockSize="
              + firstBlock.getBlock().getNumBytes() + "&genstamp="
              + firstBlock.getBlock().getGenerationStamp() + "&filename="
              + URLEncoder.encode(dir, "UTF-8") + "&datanodePort="
              + datanodePort + "&namenodeInfoPort=" + namenodeInfoPort
              + JspHelper.SET_DELEGATION + tokenString;
          resp.sendRedirect(redirectLocation);
        }
        return;
      }
      // directory
      // generate a table and dump the info
      String[] headings = { "Name", "Type", "Size", "Replication",
          "Block Size", "Modification Time", "Permission", "Owner", "Group" };
      out.print("<h3>Contents of directory ");
      JspHelper.printPathWithLinks(dir, out, namenodeInfoPort, tokenString);
      out.print("</h3><hr>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, dir);
      out.print("<hr>");

      File f = new File(dir);
      String parent;
      if ((parent = f.getParent()) != null)
        out.print("<a href=\"" + req.getRequestURL() + "?dir=" + parent
            + "&namenodeInfoPort=" + namenodeInfoPort
            + JspHelper.SET_DELEGATION + tokenString
            + "\">Go to parent directory</a><br>");

      DirectoryListing thisListing = 
        dfs.listPaths(target, HdfsFileStatus.EMPTY_NAME);
      if (thisListing == null || thisListing.getPartialListing().length == 0) {
        out.print("Empty directory");
      } else {
        JspHelper.addTableHeader(out);
        int row = 0;
        JspHelper.addTableRow(out, headings, row++);
        String cols[] = new String[headings.length];
        do {
          HdfsFileStatus[] files = thisListing.getPartialListing();
          for (int i = 0; i < files.length; i++) {
            String localFileName = files[i].getLocalName();
            // Get the location of the first block of the file
            if (!files[i].isDir()) {
              cols[1] = "file";
              cols[2] = StringUtils.byteDesc(files[i].getLen());
              cols[3] = Short.toString(files[i].getReplication());
              cols[4] = StringUtils.byteDesc(files[i].getBlockSize());
            } else {
              cols[1] = "dir";
              cols[2] = "";
              cols[3] = "";
              cols[4] = "";
            }
            String datanodeUrl = req.getRequestURL() + "?dir="
              + URLEncoder.encode(files[i].getFullName(target), "UTF-8")
              + "&namenodeInfoPort=" + namenodeInfoPort
              + JspHelper.SET_DELEGATION + tokenString;
            cols[0] = "<a href=\"" + datanodeUrl + "\">"
              + localFileName + "</a>";
            cols[5] = FsShell.dateForm.format(new Date((files[i]
              .getModificationTime())));
            cols[6] = files[i].getPermission().toString();
            cols[7] = files[i].getOwner();
            cols[8] = files[i].getGroup();
            JspHelper.addTableRow(out, cols, row++);
          }
          if (!thisListing.hasMore()) {
            break;
          }
          thisListing = dfs.listPaths(target, thisListing.getLastName());
        } while (thisListing != null);
        JspHelper.addTableFooter(out);
      }
    }
    String namenodeHost = datanode.getNameNodeAddr().getHostName();
    out.print("<br><a href=\"http://"
        + InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":"
        + namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

  static void generateFileDetails(JspWriter out, 
                                  HttpServletRequest req,
                                  Configuration conf
                                  ) throws IOException,
                                           InterruptedException {

    long startOffset = 0;
    int datanodePort;

    final Long blockId = JspHelper.validateLong(req.getParameter("blockId"));
    if (blockId == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);

    String datanodePortStr = req.getParameter("datanodePort");
    if (datanodePortStr == null) {
      out.print("Invalid input (datanodePort absent)");
      return;
    }
    datanodePort = Integer.parseInt(datanodePortStr);

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    final int chunkSizeToView = JspHelper.string2ChunkSizeToView(
        req.getParameter("chunkSizeToView"), getDefaultChunkSize(conf));

    String startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else
      startOffset = Long.parseLong(startOffsetStr);

    final String filename=JspHelper.validatePath(req.getParameter("filename"));
    if (filename == null) {
      out.print("Invalid input");
      return;
    }

    String blockSizeStr = req.getParameter("blockSize");
    long blockSize = 0;
    if (blockSizeStr == null || blockSizeStr.length() == 0) {
      out.print("Invalid input");
      return;
    }
    blockSize = Long.parseLong(blockSizeStr);

    final DFSClient dfs = getDFSClient(ugi, datanode.getNameNodeAddr(), conf);
    List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(filename, 0,
        Long.MAX_VALUE).getLocatedBlocks();
    // Add the various links for looking at the file contents
    // URL for downloading the full file
    String downloadUrl = "http://" + req.getServerName() + ":"
        + req.getServerPort() + "/streamFile?" + "filename="
        + URLEncoder.encode(filename, "UTF-8")
        + JspHelper.SET_DELEGATION + tokenString;
    out.print("<a name=\"viewOptions\"></a>");
    out.print("<a href=\"" + downloadUrl + "\">Download this file</a><br>");

    DatanodeInfo chosenNode;
    // URL for TAIL
    LocatedBlock lastBlk = blocks.get(blocks.size() - 1);
    try {
      chosenNode = JspHelper.bestNode(lastBlk);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }
    String fqdn = InetAddress.getByName(chosenNode.getHost())
        .getCanonicalHostName();
    String tailUrl = "http://" + fqdn + ":" + chosenNode.getInfoPort()
        + "/tail.jsp?filename=" + URLEncoder.encode(filename, "UTF-8")
        + "&namenodeInfoPort=" + namenodeInfoPort
        + "&chunkSizeToView=" + chunkSizeToView
        + JspHelper.SET_DELEGATION + tokenString
        + "&referrer=" + URLEncoder.encode(
            req.getRequestURL() + "?" + req.getQueryString(), "UTF-8");
    out.print("<a href=\"" + tailUrl + "\">Tail this file</a><br>");

    out.print("<form action=\"/browseBlock.jsp\" method=GET>");
    out.print("<b>Chunk size to view (in bytes, up to file's DFS block size): </b>");
    out.print("<input type=\"hidden\" name=\"blockId\" value=\"" + blockId
        + "\">");
    out.print("<input type=\"hidden\" name=\"blockSize\" value=\"" + blockSize
        + "\">");
    out.print("<input type=\"hidden\" name=\"startOffset\" value=\""
        + startOffset + "\">");
    out.print("<input type=\"hidden\" name=\"filename\" value=\"" + filename
        + "\">");
    out.print("<input type=\"hidden\" name=\"datanodePort\" value=\""
        + datanodePort + "\">");
    out.print("<input type=\"hidden\" name=\"namenodeInfoPort\" value=\""
        + namenodeInfoPort + "\">");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value="
        + chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\">");
    out.print("</form>");
    out.print("<hr>");
    out.print("<a name=\"blockDetails\"></a>");
    out.print("<B>Total number of blocks: " + blocks.size() + "</B><br>");
    // generate a table and dump the info
    out.println("\n<table>");
    
    String namenodeHost = datanode.getNameNodeAddr().getHostName();
    String namenodeHostName = InetAddress.getByName(namenodeHost).getCanonicalHostName();
    
    for (LocatedBlock cur : blocks) {
      out.print("<tr>");
      final String blockidstring = Long.toString(cur.getBlock().getBlockId());
      blockSize = cur.getBlock().getNumBytes();
      out.print("<td>" + blockidstring + ":</td>");
      DatanodeInfo[] locs = cur.getLocations();
      for (int j = 0; j < locs.length; j++) {
        String datanodeAddr = locs[j].getName();
        datanodePort = Integer.parseInt(datanodeAddr.substring(datanodeAddr
            .indexOf(':') + 1, datanodeAddr.length()));
        fqdn = InetAddress.getByName(locs[j].getHost()).getCanonicalHostName();
        String blockUrl = "http://" + fqdn + ":" + locs[j].getInfoPort()
            + "/browseBlock.jsp?blockId=" + blockidstring
            + "&blockSize=" + blockSize
            + "&filename=" + URLEncoder.encode(filename, "UTF-8")
            + "&datanodePort=" + datanodePort
            + "&genstamp=" + cur.getBlock().getGenerationStamp()
            + "&namenodeInfoPort=" + namenodeInfoPort
            + "&chunkSizeToView=" + chunkSizeToView
            + JspHelper.SET_DELEGATION + tokenString;

        String blockInfoUrl = "http://" + namenodeHostName + ":"
            + namenodeInfoPort
            + "/block_info_xml.jsp?blockId=" + blockidstring;
        out.print("<td>&nbsp</td><td><a href=\"" + blockUrl + "\">"
            + datanodeAddr + "</a></td><td>"
            + "<a href=\"" + blockInfoUrl + "\">View Block Info</a></td>");
      }
      out.println("</tr>");
    }
    out.println("</table>");
    out.print("<hr>");
    out.print("<br><a href=\"http://"
        + InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":"
        + namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

  static void generateFileChunks(JspWriter out, HttpServletRequest req,
                                 Configuration conf
                                 ) throws IOException,
                                          InterruptedException {
    long startOffset = 0;
    int datanodePort = 0;

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    final String filename = JspHelper
        .validatePath(req.getParameter("filename"));
    if (filename == null) {
      out.print("Invalid input (filename absent)");
      return;
    }

    final Long blockId = JspHelper.validateLong(req.getParameter("blockId"));
    if (blockId == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }

    final DFSClient dfs = getDFSClient(ugi, datanode.getNameNodeAddr(), conf);

    BlockAccessToken accessToken = BlockAccessToken.DUMMY_TOKEN;
    if (conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT)) {
      List<LocatedBlock> blks = dfs.getNamenode().getBlockLocations(filename, 0,
          Long.MAX_VALUE).getLocatedBlocks();
      if (blks == null || blks.size() == 0) {
        out.print("Can't locate file blocks");
        dfs.close();
        return;
      }
      for (int i = 0; i < blks.size(); i++) {
        if (blks.get(i).getBlock().getBlockId() == blockId) {
          accessToken = blks.get(i).getAccessToken();
          break;
        }
      }
    }

    final Long genStamp = JspHelper.validateLong(req.getParameter("genstamp"));
    if (genStamp == null) {
      out.print("Invalid input (genstamp absent)");
      return;
    }

    String blockSizeStr;
    long blockSize = 0;
    blockSizeStr = req.getParameter("blockSize");
    if (blockSizeStr == null) {
      out.print("Invalid input (blockSize absent)");
      return;
    }
    blockSize = Long.parseLong(blockSizeStr);

    final int chunkSizeToView = JspHelper.string2ChunkSizeToView(req
        .getParameter("chunkSizeToView"), getDefaultChunkSize(conf));

    String startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else
      startOffset = Long.parseLong(startOffsetStr);

    String datanodePortStr = req.getParameter("datanodePort");
    if (datanodePortStr == null) {
      out.print("Invalid input (datanodePort absent)");
      return;
    }
    datanodePort = Integer.parseInt(datanodePortStr);
    out.print("<h3>File: ");
    JspHelper.printPathWithLinks(filename, out, namenodeInfoPort,
                                 tokenString);
    out.print("</h3><hr>");
    String parent = new File(filename).getParent();
    JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, parent);
    out.print("<hr>");
    out.print("<a href=\"http://"
        + req.getServerName() + ":" + req.getServerPort()
        + "/browseDirectory.jsp?dir=" + URLEncoder.encode(parent, "UTF-8")
        + "&namenodeInfoPort=" + namenodeInfoPort
        + JspHelper.SET_DELEGATION + tokenString
        + "\"><i>Go back to dir listing</i></a><br>");
    out.print("<a href=\"#viewOptions\">Advanced view/download options</a><br>");
    out.print("<hr>");

    // Determine the prev & next blocks
    long nextStartOffset = 0;
    long nextBlockSize = 0;
    String nextBlockIdStr = null;
    String nextGenStamp = null;
    String nextHost = req.getServerName();
    int nextPort = req.getServerPort();
    int nextDatanodePort = datanodePort;
    // determine data for the next link
    if (startOffset + chunkSizeToView >= blockSize) {
      // we have to go to the next block from this point onwards
      List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(filename, 0,
          Long.MAX_VALUE).getLocatedBlocks();
      for (int i = 0; i < blocks.size(); i++) {
        if (blocks.get(i).getBlock().getBlockId() == blockId) {
          if (i != blocks.size() - 1) {
            LocatedBlock nextBlock = blocks.get(i + 1);
            nextBlockIdStr = Long.toString(nextBlock.getBlock().getBlockId());
            nextGenStamp = Long.toString(nextBlock.getBlock()
                .getGenerationStamp());
            nextStartOffset = 0;
            nextBlockSize = nextBlock.getBlock().getNumBytes();
            DatanodeInfo d = JspHelper.bestNode(nextBlock);
            String datanodeAddr = d.getName();
            nextDatanodePort = Integer.parseInt(datanodeAddr.substring(
                datanodeAddr.indexOf(':') + 1, datanodeAddr.length()));
            nextHost = InetAddress.getByName(d.getHost())
                .getCanonicalHostName();
            nextPort = d.getInfoPort();
          }
        }
      }
    } else {
      // we are in the same block
      nextBlockIdStr = blockId.toString();
      nextStartOffset = startOffset + chunkSizeToView;
      nextBlockSize = blockSize;
      nextGenStamp = genStamp.toString();
    }
    String nextUrl = null;
    if (nextBlockIdStr != null) {
      nextUrl = "http://" + nextHost + ":" + nextPort
          + "/browseBlock.jsp?blockId=" + nextBlockIdStr
          + "&blockSize=" + nextBlockSize
          + "&startOffset=" + nextStartOffset
          + "&genstamp=" + nextGenStamp
          + "&filename=" + URLEncoder.encode(filename, "UTF-8")
          + "&chunkSizeToView=" + chunkSizeToView
          + "&datanodePort=" + nextDatanodePort
          + "&namenodeInfoPort=" + namenodeInfoPort
          + JspHelper.SET_DELEGATION + tokenString;
      out.print("<a href=\"" + nextUrl + "\">View Next chunk</a>&nbsp;&nbsp;");
    }
    // determine data for the prev link
    String prevBlockIdStr = null;
    String prevGenStamp = null;
    long prevStartOffset = 0;
    long prevBlockSize = 0;
    String prevHost = req.getServerName();
    int prevPort = req.getServerPort();
    int prevDatanodePort = datanodePort;
    if (startOffset == 0) {
      List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(filename, 0,
          Long.MAX_VALUE).getLocatedBlocks();
      for (int i = 0; i < blocks.size(); i++) {
        if (blocks.get(i).getBlock().getBlockId() == blockId) {
          if (i != 0) {
            LocatedBlock prevBlock = blocks.get(i - 1);
            prevBlockIdStr = Long.toString(prevBlock.getBlock().getBlockId());
            prevGenStamp = Long.toString(prevBlock.getBlock()
                .getGenerationStamp());
            prevStartOffset = prevBlock.getBlock().getNumBytes()
                - chunkSizeToView;
            if (prevStartOffset < 0)
              prevStartOffset = 0;
            prevBlockSize = prevBlock.getBlock().getNumBytes();
            DatanodeInfo d = JspHelper.bestNode(prevBlock);
            String datanodeAddr = d.getName();
            prevDatanodePort = Integer.parseInt(datanodeAddr.substring(
                datanodeAddr.indexOf(':') + 1, datanodeAddr.length()));
            prevHost = InetAddress.getByName(d.getHost())
                .getCanonicalHostName();
            prevPort = d.getInfoPort();
          }
        }
      }
    } else {
      // we are in the same block
      prevBlockIdStr = blockId.toString();
      prevStartOffset = startOffset - chunkSizeToView;
      if (prevStartOffset < 0)
        prevStartOffset = 0;
      prevBlockSize = blockSize;
      prevGenStamp = genStamp.toString();
    }

    String prevUrl = null;
    if (prevBlockIdStr != null) {
      prevUrl = "http://" + prevHost + ":" + prevPort
          + "/browseBlock.jsp?blockId=" + prevBlockIdStr
          + "&blockSize=" + prevBlockSize
          + "&startOffset=" + prevStartOffset
          + "&filename=" + URLEncoder.encode(filename, "UTF-8")
          + "&chunkSizeToView=" + chunkSizeToView
          + "&genstamp=" + prevGenStamp
          + "&datanodePort=" + prevDatanodePort
          + "&namenodeInfoPort=" + namenodeInfoPort
          + JspHelper.SET_DELEGATION + tokenString;
      out.print("<a href=\"" + prevUrl + "\">View Prev chunk</a>&nbsp;&nbsp;");
    }
    out.print("<hr>");
    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" style=\"width:100%\" READONLY>");
    try {
      JspHelper.streamBlockInAscii(new InetSocketAddress(req.getServerName(),
          datanodePort), blockId, accessToken, genStamp, blockSize,
          startOffset, chunkSizeToView, out, conf);
    } catch (Exception e) {
      out.print(e);
    }
    out.print("</textarea>");
    dfs.close();
  }

  static void generateFileChunksForTail(JspWriter out, HttpServletRequest req,
                                        Configuration conf
                                        ) throws IOException,
                                                 InterruptedException {
    final String referrer = JspHelper.validateURL(req.getParameter("referrer"));
    boolean noLink = false;
    if (referrer == null) {
      noLink = true;
    }

    final String filename = JspHelper
        .validatePath(req.getParameter("filename"));
    if (filename == null) {
      out.print("Invalid input (file name absent)");
      return;
    }
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    final int chunkSizeToView = JspHelper.string2ChunkSizeToView(req
        .getParameter("chunkSizeToView"), getDefaultChunkSize(conf));

    if (!noLink) {
      out.print("<h3>Tail of File: ");
      JspHelper.printPathWithLinks(filename, out, namenodeInfoPort, 
                                   tokenString);
      out.print("</h3><hr>");
      out.print("<a href=\"" + referrer + "\">Go Back to File View</a><hr>");
    } else {
      out.print("<h3>" + filename + "</h3>");
    }
    out.print("<b>Chunk size to view (in bytes, up to file's DFS block size): </b>");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value="
        + chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\"><hr>");
    out.print("<input type=\"hidden\" name=\"filename\" value=\"" + filename
        + "\">");
    out.print("<input type=\"hidden\" name=\"namenodeInfoPort\" value=\""
        + namenodeInfoPort + "\">");
    if (!noLink)
      out.print("<input type=\"hidden\" name=\"referrer\" value=\"" + referrer
          + "\">");

    // fetch the block from the datanode that has the last block for this file
    final DFSClient dfs = getDFSClient(ugi, datanode.getNameNodeAddr(), conf);
    List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(filename, 0,
        Long.MAX_VALUE).getLocatedBlocks();
    if (blocks == null || blocks.size() == 0) {
      out.print("No datanodes contain blocks of file " + filename);
      dfs.close();
      return;
    }
    LocatedBlock lastBlk = blocks.get(blocks.size() - 1);
    long blockSize = lastBlk.getBlock().getNumBytes();
    long blockId = lastBlk.getBlock().getBlockId();
    BlockAccessToken accessToken = lastBlk.getAccessToken();
    long genStamp = lastBlk.getBlock().getGenerationStamp();
    DatanodeInfo chosenNode;
    try {
      chosenNode = JspHelper.bestNode(lastBlk);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }
    InetSocketAddress addr = NetUtils.createSocketAddr(chosenNode.getName());
    // view the last chunkSizeToView bytes while Tailing
    final long startOffset = blockSize >= chunkSizeToView ? blockSize
        - chunkSizeToView : 0;

    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" style=\"width:100%\" READONLY>");
    JspHelper.streamBlockInAscii(addr, blockId, accessToken, genStamp,
        blockSize, startOffset, chunkSizeToView, out, conf);
    out.print("</textarea>");
    dfs.close();
  }
}
