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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

@InterfaceAudience.Private
public class DatanodeJspHelper {
  private static final int PREV_BLOCK = -1;
  private static final int NEXT_BLOCK = 1;

  private static DFSClient getDFSClient(final UserGroupInformation user,
                                        final String addr,
                                        final Configuration conf
                                        ) throws IOException,
                                                 InterruptedException {
    return
      user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        @Override
        public DFSClient run() throws IOException {
          return new DFSClient(NetUtils.createSocketAddr(addr), conf);
        }
      });
  }

  /**
   * Get the default chunk size.
   * @param conf the configuration
   * @return the number of bytes to chunk in
   */
  private static int getDefaultChunkSize(Configuration conf) {
    return conf.getInt(DFSConfigKeys.DFS_DEFAULT_CHUNK_VIEW_SIZE_KEY,
                       DFSConfigKeys.DFS_DEFAULT_CHUNK_VIEW_SIZE_DEFAULT);
  }

  static void generateDirectoryStructure(JspWriter out, 
                                         HttpServletRequest req,
                                         HttpServletResponse resp,
                                         Configuration conf
                                         ) throws IOException,
                                                  InterruptedException {
    final String dir = JspHelper.validatePath(
        StringEscapeUtils.unescapeHtml(req.getParameter("dir")));
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
    final String nnAddr = req.getParameter(JspHelper.NAMENODE_ADDRESS);
    if (nnAddr == null){
      out.print(JspHelper.NAMENODE_ADDRESS + " url param is null");
      return;
    }
    
    DFSClient dfs = getDFSClient(ugi, nnAddr, conf);
    String target = dir;
    final HdfsFileStatus targetStatus = dfs.getFileInfo(target);
    if (targetStatus == null) { // not exists
      out.print("<h3>File or directory : " + StringEscapeUtils.escapeHtml(target) + " does not exist</h3>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, target,
          nnAddr);
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
          DatanodeInfo chosenNode = JspHelper.bestNode(firstBlock, conf);
          int datanodePort = chosenNode.getXferPort();
          String redirectLocation = JspHelper.Url.url(req.getScheme(),
              chosenNode)
              + "/browseBlock.jsp?blockId="
              + firstBlock.getBlock().getBlockId() + "&blockSize="
              + firstBlock.getBlock().getNumBytes() + "&genstamp="
              + firstBlock.getBlock().getGenerationStamp() + "&filename="
              + URLEncoder.encode(dir, "UTF-8") + "&datanodePort="
              + datanodePort + "&namenodeInfoPort=" + namenodeInfoPort
              + JspHelper.getDelegationTokenUrlParam(tokenString)
              + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr);
          resp.sendRedirect(redirectLocation);
        }
        return;
      }
      // directory
      // generate a table and dump the info
      String[] headings = { "Name", "Type", "Size", "Replication",
          "Block Size", "Modification Time", "Permission", "Owner", "Group" };
      out.print("<h3>Contents of directory ");
      JspHelper.printPathWithLinks(dir, out, namenodeInfoPort, tokenString,
          nnAddr);
      out.print("</h3><hr>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, dir, nnAddr);
      out.print("<hr>");

      File f = new File(dir);
      String parent;
      if ((parent = f.getParent()) != null)
        out.print("<a href=\"" + req.getRequestURL() + "?dir=" + parent
            + "&namenodeInfoPort=" + namenodeInfoPort
            + JspHelper.getDelegationTokenUrlParam(tokenString)
            + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr)
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
              + JspHelper.getDelegationTokenUrlParam(tokenString)
              + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr);
            cols[0] = "<a href=\"" + datanodeUrl + "\">"
              + HtmlQuoting.quoteHtmlChars(localFileName) + "</a>";
            cols[5] = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(
                new Date((files[i].getModificationTime())));
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
    out.print("<br><a href=\"///"
        + JspHelper.canonicalize(nnAddr) + ":"
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

    final Long genStamp = JspHelper.validateLong(req.getParameter("genstamp"));
    if (genStamp == null) {
      out.print("Invalid input (genstamp absent)");
      return;
    }
    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);
    final String nnAddr = StringEscapeUtils.escapeHtml(
        req.getParameter(JspHelper.NAMENODE_ADDRESS));
    if (nnAddr == null){
      out.print(JspHelper.NAMENODE_ADDRESS + " url param is null");
      return;
    }

    final int chunkSizeToView = JspHelper.string2ChunkSizeToView(
        req.getParameter("chunkSizeToView"), getDefaultChunkSize(conf));

    String startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else
      startOffset = Long.parseLong(startOffsetStr);

    String path = StringEscapeUtils.unescapeHtml(req.getParameter("filename"));
    if (path == null) {
      path = req.getPathInfo() == null ? "/" : req.getPathInfo();
    }
    final String filename = JspHelper.validatePath(path);
    if (filename == null) {
      out.print("Invalid input");
      return;
    }

    final String blockSizeStr = req.getParameter("blockSize");
    if (blockSizeStr == null || blockSizeStr.length() == 0) {
      out.print("Invalid input");
      return;
    }
    long blockSize = Long.parseLong(blockSizeStr);

    final DFSClient dfs = getDFSClient(ugi, nnAddr, conf);
    List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(filename, 0,
        Long.MAX_VALUE).getLocatedBlocks();
    // Add the various links for looking at the file contents
    // URL for downloading the full file
    String downloadUrl = "/streamFile" + ServletUtil.encodePath(filename)
        + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr, true)
        + JspHelper.getDelegationTokenUrlParam(tokenString);
    out.print("<a name=\"viewOptions\"></a>");
    out.print("<a href=\"" + downloadUrl + "\">Download this file</a><br>");

    DatanodeInfo chosenNode;
    // URL for TAIL
    LocatedBlock lastBlk = blocks.get(blocks.size() - 1);
    try {
      chosenNode = JspHelper.bestNode(lastBlk, conf);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }

    String tailUrl = "///" + JspHelper.Url.authority(req.getScheme(), chosenNode)
        + "/tail.jsp?filename=" + URLEncoder.encode(filename, "UTF-8")
        + "&namenodeInfoPort=" + namenodeInfoPort
        + "&chunkSizeToView=" + chunkSizeToView
        + JspHelper.getDelegationTokenUrlParam(tokenString)
        + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr)
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
    out.print("<input type=\"hidden\" name=\"genstamp\" value=\"" + genStamp
        + "\">");
    out.print("<input type=\"hidden\" name=\"datanodePort\" value=\""
        + datanodePort + "\">");
    out.print("<input type=\"hidden\" name=\"namenodeInfoPort\" value=\""
        + namenodeInfoPort + "\">");
    out.print("<input type=\"hidden\" name=\"" + JspHelper.NAMENODE_ADDRESS
        + "\" value=\"" + nnAddr + "\">");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value="
        + chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\">");
    out.print("</form>");
    out.print("<hr>");
    out.print("<a name=\"blockDetails\"></a>");
    out.print("<B>Total number of blocks: " + blocks.size() + "</B><br>");
    // generate a table and dump the info
    out.println("\n<table>");
    
    String nnCanonicalName = JspHelper.canonicalize(nnAddr);
    for (LocatedBlock cur : blocks) {
      out.print("<tr>");
      final String blockidstring = Long.toString(cur.getBlock().getBlockId());
      blockSize = cur.getBlock().getNumBytes();
      out.print("<td>" + blockidstring + ":</td>");
      DatanodeInfo[] locs = cur.getLocations();
      for (int j = 0; j < locs.length; j++) {
        String datanodeAddr = locs[j].getXferAddr();
        datanodePort = locs[j].getXferPort();
        String blockUrl = "///" + JspHelper.Url.authority(req.getScheme(), locs[j])
            + "/browseBlock.jsp?blockId=" + blockidstring
            + "&blockSize=" + blockSize
            + "&filename=" + URLEncoder.encode(filename, "UTF-8")
            + "&datanodePort=" + datanodePort
            + "&genstamp=" + cur.getBlock().getGenerationStamp()
            + "&namenodeInfoPort=" + namenodeInfoPort
            + "&chunkSizeToView=" + chunkSizeToView
            + JspHelper.getDelegationTokenUrlParam(tokenString)
            + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr);

        String blockInfoUrl = "///" + nnCanonicalName + ":"
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
    out.print("<br><a href=\"///"
        + nnCanonicalName + ":"
        + namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

  static void generateFileChunks(JspWriter out, HttpServletRequest req,
                                 Configuration conf
                                 ) throws IOException,
                                          InterruptedException {
    long startOffset = 0;
    int datanodePort = 0;

    final String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    final String nnAddr = req.getParameter(JspHelper.NAMENODE_ADDRESS);
    if (nnAddr == null) {
      out.print(JspHelper.NAMENODE_ADDRESS + " url param is null");
      return;
    }
    final String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    final String filename = JspHelper
        .validatePath(StringEscapeUtils.unescapeHtml(req.getParameter("filename")));
    if (filename == null) {
      out.print("Invalid input (filename absent)");
      return;
    }
    
    final Long blockId = JspHelper.validateLong(req.getParameter("blockId"));
    if (blockId == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }
    
    final DFSClient dfs = getDFSClient(ugi, nnAddr, conf);

    String bpid = null;
    Token<BlockTokenIdentifier> blockToken = BlockTokenSecretManager.DUMMY_TOKEN;
    List<LocatedBlock> blks = dfs.getNamenode().getBlockLocations(filename, 0,
        Long.MAX_VALUE).getLocatedBlocks();
    if (blks == null || blks.size() == 0) {
      out.print("Can't locate file blocks");
      dfs.close();
      return;
    }

    boolean needBlockToken = conf.getBoolean(
            DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
            DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);

    for (int i = 0; i < blks.size(); i++) {
      if (blks.get(i).getBlock().getBlockId() == blockId) {
        bpid = blks.get(i).getBlock().getBlockPoolId();
        if (needBlockToken) {
          blockToken = blks.get(i).getBlockToken();
        }
        break;
      }
    }

    final Long genStamp = JspHelper.validateLong(req.getParameter("genstamp"));
    if (genStamp == null) {
      out.print("Invalid input (genstamp absent)");
      return;
    }

    long blockSize = 0;
    final String blockSizeStr = req.getParameter("blockSize");
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
                                 tokenString, nnAddr);
    out.print("</h3><hr>");
    String parent = new File(filename).getParent();
    JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, parent, nnAddr);
    out.print("<hr>");
    out.print("<a href=\"/browseDirectory.jsp?dir=" + URLEncoder.encode(parent, "UTF-8")
        + "&namenodeInfoPort=" + namenodeInfoPort
        + JspHelper.getDelegationTokenUrlParam(tokenString)
        + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr)
        + "\"><i>Go back to dir listing</i></a><br>");
    out.print("<a href=\"#viewOptions\">Advanced view/download options</a><br>");
    out.print("<hr>");

    String authority = req.getServerName() + ":" + req.getServerPort();
    String nextUrl = generateLinksForAdjacentBlock(NEXT_BLOCK, authority,
        datanodePort, startOffset, chunkSizeToView, blockSize, blockId,
        genStamp, dfs, filename, conf, req.getScheme(), tokenString,
        namenodeInfoPort, nnAddr);
    if (nextUrl != null) {
      out.print("<a href=\"" + nextUrl + "\">View Next chunk</a>&nbsp;&nbsp;");
    }

    String prevUrl = generateLinksForAdjacentBlock(PREV_BLOCK, authority,
        datanodePort, startOffset, chunkSizeToView, blockSize, blockId,
        genStamp, dfs, filename, conf, req.getScheme(), tokenString,
        namenodeInfoPort, nnAddr);
    if (prevUrl != null) {
      out.print("<a href=\"" + prevUrl + "\">View Prev chunk</a>&nbsp;&nbsp;");
    }

    out.print("<hr>");
    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" style=\"width:100%\" READONLY>");
    try {
      JspHelper.streamBlockInAscii(new InetSocketAddress(req.getServerName(),
          datanodePort), bpid, blockId, blockToken, genStamp, blockSize,
          startOffset, chunkSizeToView, out, conf, dfs.getConf(),
          dfs, getSaslDataTransferClient(req));
    } catch (Exception e) {
      out.print(e);
    }
    out.print("</textarea>");
    dfs.close();
  }

  private static String generateLinksForAdjacentBlock(final int direction,
      String authority, int datanodePort, long startOffset,
      int chunkSizeToView, long blockSize, long blockId, Long genStamp,
      final DFSClient dfs, final String filename, final Configuration conf,
      final String scheme, final String tokenString,
      final int namenodeInfoPort, final String nnAddr)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {

    boolean found = false;
    if ((direction == NEXT_BLOCK && startOffset + chunkSizeToView < blockSize)
        || (direction == PREV_BLOCK && startOffset != 0)) {
      // we are in the same block
      found = true;

      if (direction == NEXT_BLOCK) {
        startOffset = startOffset + chunkSizeToView;
      } else {
        startOffset = Math.max(0, startOffset - chunkSizeToView);
      }
    } else {
      List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(filename, 0,
          Long.MAX_VALUE).getLocatedBlocks();

      final long curBlockId = blockId;
      int curBlockIdx = Iterables.indexOf(blocks, new Predicate<LocatedBlock>() {
        @Override
        public boolean apply(LocatedBlock b) {
          return b.getBlock().getBlockId() == curBlockId;
        }
      });
      found = curBlockIdx != -1 &&
          ((direction == NEXT_BLOCK && curBlockIdx < blocks.size() - 1)
              || (direction == PREV_BLOCK && curBlockIdx > 0));

      if (found) {
        LocatedBlock nextBlock = blocks.get(curBlockIdx + direction);

        blockId = nextBlock.getBlock().getBlockId();
        genStamp = nextBlock.getBlock().getGenerationStamp();
        startOffset = 0;
        blockSize = nextBlock.getBlock().getNumBytes();
        DatanodeInfo d = JspHelper.bestNode(nextBlock, conf);
        datanodePort = d.getXferPort();
        authority = JspHelper.Url.authority(scheme, d);
      }
    }

    if (found) {
      return "///" + authority
          + "/browseBlock.jsp?blockId=" + blockId
          + "&blockSize=" + blockSize
          + "&startOffset=" + startOffset
          + "&genstamp=" + genStamp
          + "&filename=" + URLEncoder.encode(filename, "UTF-8")
          + "&chunkSizeToView=" + chunkSizeToView
          + "&datanodePort=" + datanodePort
          + "&namenodeInfoPort=" + namenodeInfoPort
          + JspHelper.getDelegationTokenUrlParam(tokenString)
          + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnAddr);
    } else {
      return null;
    }
  }

  static void generateFileChunksForTail(JspWriter out, HttpServletRequest req,
                                        Configuration conf
                                        ) throws IOException,
                                                 InterruptedException {
    String referrer = null;
    boolean noLink = false;
    try {
      referrer = new URL(req.getParameter("referrer")).toString();
    } catch (IOException e) {
      referrer = null;
      noLink = true;
    }

    final String filename = JspHelper
        .validatePath(StringEscapeUtils.unescapeHtml(req.getParameter("filename")));
    if (filename == null) {
      out.print("Invalid input (file name absent)");
      return;
    }
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    String nnAddr = StringEscapeUtils.escapeHtml(req.getParameter(JspHelper.NAMENODE_ADDRESS));
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    final int chunkSizeToView = JspHelper.string2ChunkSizeToView(req
        .getParameter("chunkSizeToView"), getDefaultChunkSize(conf));

    if (!noLink) {
      out.print("<h3>Tail of File: ");
      JspHelper.printPathWithLinks(filename, out, namenodeInfoPort, 
                                   tokenString, nnAddr);
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
    out.print("<input type=\"hidden\" name=\"" + JspHelper.NAMENODE_ADDRESS
        + "\" value=\"" + nnAddr + "\">");
    if (!noLink)
      out.print("<input type=\"hidden\" name=\"referrer\" value=\"" + referrer
          + "\">");

    // fetch the block from the datanode that has the last block for this file
    final DFSClient dfs = getDFSClient(ugi, nnAddr, conf);
    List<LocatedBlock> blocks = dfs.getNamenode().getBlockLocations(filename, 0,
        Long.MAX_VALUE).getLocatedBlocks();
    if (blocks == null || blocks.size() == 0) {
      out.print("No datanodes contain blocks of file " + filename);
      dfs.close();
      return;
    }
    LocatedBlock lastBlk = blocks.get(blocks.size() - 1);
    String poolId = lastBlk.getBlock().getBlockPoolId();
    long blockSize = lastBlk.getBlock().getNumBytes();
    long blockId = lastBlk.getBlock().getBlockId();
    Token<BlockTokenIdentifier> accessToken = lastBlk.getBlockToken();
    long genStamp = lastBlk.getBlock().getGenerationStamp();
    DatanodeInfo chosenNode;
    try {
      chosenNode = JspHelper.bestNode(lastBlk, conf);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }
    InetSocketAddress addr = 
      NetUtils.createSocketAddr(chosenNode.getXferAddr());
    // view the last chunkSizeToView bytes while Tailing
    final long startOffset = blockSize >= chunkSizeToView ? blockSize
        - chunkSizeToView : 0;

    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" style=\"width:100%\" READONLY>");
    JspHelper.streamBlockInAscii(addr, poolId, blockId, accessToken, genStamp,
        blockSize, startOffset, chunkSizeToView, out, conf, dfs.getConf(),
        dfs, getSaslDataTransferClient(req));
    out.print("</textarea>");
    dfs.close();
  }
  
  
  /** Get DFSClient for a namenode corresponding to the BPID from a datanode */
  public static DFSClient getDFSClient(final HttpServletRequest request,
      final DataNode datanode, final Configuration conf,
      final UserGroupInformation ugi) throws IOException, InterruptedException {
    final String nnAddr = request.getParameter(JspHelper.NAMENODE_ADDRESS);
    return getDFSClient(ugi, nnAddr, conf);
  }

  /** Return a table containing version information. */
  public static String getVersionTable(ServletContext context) {
    StringBuilder sb = new StringBuilder();
    final DataNode dataNode = (DataNode) context.getAttribute("datanode");
    sb.append("<div class='dfstable'><table>");
    sb.append("<tr><td class='col1'>Version:</td><td>");
    sb.append(VersionInfo.getVersion() + ", " + VersionInfo.getRevision());
    sb.append("</td></tr>\n" + "\n  <tr><td class='col1'>Compiled:</td><td>"
        + VersionInfo.getDate());
    sb.append(" by " + VersionInfo.getUser() + " from "
        + VersionInfo.getBranch());
    if (dataNode != null) {
      sb.append("</td></tr>\n  <tr><td class='col1'>Cluster ID:</td><td>"
          + dataNode.getClusterId());
    }
    sb.append("</td></tr>\n</table></div>");
    return sb.toString();
  }

  /**
   * Gets the {@link SaslDataTransferClient} from the {@link DataNode} attached
   * to the servlet context.
   *
   * @return SaslDataTransferClient from DataNode
   */
  private static SaslDataTransferClient getSaslDataTransferClient(
      HttpServletRequest req) {
    DataNode dataNode = (DataNode)req.getSession().getServletContext()
      .getAttribute("datanode");
    return dataNode.saslClient;
  }
}
