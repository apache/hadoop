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

package org.apache.hadoop.hdfs.server.common;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.ProxyServers;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.VersionInfo;

import com.google.common.base.Charsets;

@InterfaceAudience.Private
public class JspHelper {
  public static final String CURRENT_CONF = "current.conf";
  public static final String DELEGATION_PARAMETER_NAME = DelegationParam.NAME;
  public static final String NAMENODE_ADDRESS = "nnaddr";
  static final String SET_DELEGATION = "&" + DELEGATION_PARAMETER_NAME +
                                              "=";
  private static final Log LOG = LogFactory.getLog(JspHelper.class);

  /** Private constructor for preventing creating JspHelper object. */
  private JspHelper() {} 
  
  // data structure to count number of blocks on datanodes.
  private static class NodeRecord extends DatanodeInfo {
    int frequency;

    public NodeRecord(DatanodeInfo info, int count) {
      super(info);
      this.frequency = count;
    }
    
    @Override
    public boolean equals(Object obj) {
      // Sufficient to use super equality as datanodes are uniquely identified
      // by DatanodeID
      return (this == obj) || super.equals(obj);
    }
    @Override
    public int hashCode() {
      // Super implementation is sufficient
      return super.hashCode();
    }
  }

  // compare two records based on their frequency
  private static class NodeRecordComparator implements Comparator<NodeRecord> {

    @Override
    public int compare(NodeRecord o1, NodeRecord o2) {
      if (o1.frequency < o2.frequency) {
        return -1;
      } else if (o1.frequency > o2.frequency) {
        return 1;
      } 
      return 0;
    }
  }
  
  /**
   * convenience method for canonicalizing host name.
   * @param addr name:port or name 
   * @return canonicalized host name
   */
   public static String canonicalize(String addr) {
    // default port 1 is supplied to allow addr without port.
    // the port will be ignored.
    return NetUtils.createSocketAddr(addr, 1).getAddress()
           .getCanonicalHostName();
  }

  /**
   * A helper class that generates the correct URL for different schema.
   *
   */
  public static final class Url {
    public static String authority(String scheme, DatanodeID d) {
      String fqdn = (d.getIpAddr() != null && !d.getIpAddr().isEmpty())?
          canonicalize(d.getIpAddr()): 
          d.getHostName();
      if (scheme.equals("http")) {
        return fqdn + ":" + d.getInfoPort();
      } else if (scheme.equals("https")) {
        return fqdn + ":" + d.getInfoSecurePort();
      } else {
        throw new IllegalArgumentException("Unknown scheme:" + scheme);
      }
    }

    public static String url(String scheme, DatanodeID d) {
      return scheme + "://" + authority(scheme, d);
    }
  }

  public static DatanodeInfo bestNode(LocatedBlocks blks, Configuration conf)
      throws IOException {
    HashMap<DatanodeInfo, NodeRecord> map =
      new HashMap<DatanodeInfo, NodeRecord>();
    for (LocatedBlock block : blks.getLocatedBlocks()) {
      DatanodeInfo[] nodes = block.getLocations();
      for (DatanodeInfo node : nodes) {
        NodeRecord record = map.get(node);
        if (record == null) {
          map.put(node, new NodeRecord(node, 1));
        } else {
          record.frequency++;
        }
      }
    }
    NodeRecord[] nodes = map.values().toArray(new NodeRecord[map.size()]);
    Arrays.sort(nodes, new NodeRecordComparator());
    return bestNode(nodes, false);
  }

  public static DatanodeInfo bestNode(LocatedBlock blk, Configuration conf)
      throws IOException {
    DatanodeInfo[] nodes = blk.getLocations();
    return bestNode(nodes, true);
  }

  private static DatanodeInfo bestNode(DatanodeInfo[] nodes, boolean doRandom)
      throws IOException {
    if (nodes == null || nodes.length == 0) {
      throw new IOException("No nodes contain this block");
    }
    int l = 0;
    while (l < nodes.length && !nodes[l].isDecommissioned()) {
      ++l;
    }

    if (l == 0) {
      throw new IOException("No active nodes contain this block");
    }

    int index = doRandom ? DFSUtil.getRandom().nextInt(l) : 0;
    return nodes[index];
  }

  public static void streamBlockInAscii(InetSocketAddress addr, String poolId,
      long blockId, final Token<BlockTokenIdentifier> blockToken, long genStamp,
      long blockSize, long offsetIntoBlock, long chunkSizeToView,
      JspWriter out, final Configuration conf, DFSClient.Conf dfsConf,
      final DFSClient dfs, final SaslDataTransferClient saslClient)
          throws IOException {
    if (chunkSizeToView == 0) return;
    int amtToRead = (int)Math.min(chunkSizeToView, blockSize - offsetIntoBlock);
      
    DatanodeID datanodeId = new DatanodeID(addr.getAddress().getHostAddress(),
      addr.getHostName(), poolId, addr.getPort(), 0, 0, 0);
    BlockReader blockReader = new BlockReaderFactory(dfsConf).
      setInetSocketAddress(addr).
      setBlock(new ExtendedBlock(poolId, blockId, 0, genStamp)).
      setFileName(BlockReaderFactory.getFileName(addr, poolId, blockId)).
      setBlockToken(blockToken).
      setStartOffset(offsetIntoBlock).
      setLength(amtToRead).
      setVerifyChecksum(true).
      setClientName("JspHelper").
      setClientCacheContext(ClientContext.getFromConf(conf)).
      setDatanodeInfo(new DatanodeInfo(datanodeId)).
      setCachingStrategy(CachingStrategy.newDefaultStrategy()).
      setConfiguration(conf).
      setRemotePeerFactory(new RemotePeerFactory() {
        @Override
        public Peer newConnectedPeer(InetSocketAddress addr,
            Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
            throws IOException {
          Peer peer = null;
          Socket sock = NetUtils.getDefaultSocketFactory(conf).createSocket();
          try {
            sock.connect(addr, HdfsServerConstants.READ_TIMEOUT);
            sock.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
            peer = TcpPeerServer.peerFromSocketAndKey(saslClient, sock, dfs,
                blockToken, datanodeId);
          } finally {
            if (peer == null) {
              IOUtils.closeSocket(sock);
            }
          }
          return peer;
        }
      }).
      build();

    final byte[] buf = new byte[amtToRead];
    try {
      int readOffset = 0;
      int retries = 2;
      while (amtToRead > 0) {
        int numRead = amtToRead;
        try {
          blockReader.readFully(buf, readOffset, amtToRead);
        } catch (IOException e) {
          retries--;
          if (retries == 0)
            throw new IOException("Could not read data from datanode");
          continue;
        }
        amtToRead -= numRead;
        readOffset += numRead;
      }
    } finally {
      blockReader.close();
    }
    out.print(HtmlQuoting.quoteHtmlChars(new String(buf, Charsets.UTF_8)));
  }

  public static void addTableHeader(JspWriter out) throws IOException {
    out.print("<table border=\"1\""+
              " cellpadding=\"2\" cellspacing=\"2\">");
    out.print("<tbody>");
  }
  public static void addTableRow(JspWriter out, String[] columns) throws IOException {
    out.print("<tr>");
    for (int i = 0; i < columns.length; i++) {
      out.print("<td style=\"vertical-align: top;\"><B>"+columns[i]+"</B><br></td>");
    }
    out.print("</tr>");
  }
  public static void addTableRow(JspWriter out, String[] columns, int row) throws IOException {
    out.print("<tr>");
      
    for (int i = 0; i < columns.length; i++) {
      if (row/2*2 == row) {//even
        out.print("<td style=\"vertical-align: top;background-color:LightGrey;\"><B>"+columns[i]+"</B><br></td>");
      } else {
        out.print("<td style=\"vertical-align: top;background-color:LightBlue;\"><B>"+columns[i]+"</B><br></td>");
          
      }
    }
    out.print("</tr>");
  }
  public static void addTableFooter(JspWriter out) throws IOException {
    out.print("</tbody></table>");
  }

  public static void sortNodeList(final List<DatanodeDescriptor> nodes,
                           String field, String order) {
        
    class NodeComapare implements Comparator<DatanodeDescriptor> {
      static final int 
        FIELD_NAME              = 1,
        FIELD_LAST_CONTACT      = 2,
        FIELD_BLOCKS            = 3,
        FIELD_CAPACITY          = 4,
        FIELD_USED              = 5,
        FIELD_PERCENT_USED      = 6,
        FIELD_NONDFS_USED       = 7,
        FIELD_REMAINING         = 8,
        FIELD_PERCENT_REMAINING = 9,
        FIELD_ADMIN_STATE       = 10,
        FIELD_DECOMMISSIONED    = 11,
        SORT_ORDER_ASC          = 1,
        SORT_ORDER_DSC          = 2;

      int sortField = FIELD_NAME;
      int sortOrder = SORT_ORDER_ASC;
            
      public NodeComapare(String field, String order) {
        if (field.equals("lastcontact")) {
          sortField = FIELD_LAST_CONTACT;
        } else if (field.equals("capacity")) {
          sortField = FIELD_CAPACITY;
        } else if (field.equals("used")) {
          sortField = FIELD_USED;
        } else if (field.equals("nondfsused")) {
          sortField = FIELD_NONDFS_USED;
        } else if (field.equals("remaining")) {
          sortField = FIELD_REMAINING;
        } else if (field.equals("pcused")) {
          sortField = FIELD_PERCENT_USED;
        } else if (field.equals("pcremaining")) {
          sortField = FIELD_PERCENT_REMAINING;
        } else if (field.equals("blocks")) {
          sortField = FIELD_BLOCKS;
        } else if (field.equals("adminstate")) {
          sortField = FIELD_ADMIN_STATE;
        } else if (field.equals("decommissioned")) {
          sortField = FIELD_DECOMMISSIONED;
        } else {
          sortField = FIELD_NAME;
        }
                
        if (order.equals("DSC")) {
          sortOrder = SORT_ORDER_DSC;
        } else {
          sortOrder = SORT_ORDER_ASC;
        }
      }

      @Override
      public int compare(DatanodeDescriptor d1,
                         DatanodeDescriptor d2) {
        int ret = 0;
        switch (sortField) {
        case FIELD_LAST_CONTACT:
          ret = (int) (d2.getLastUpdate() - d1.getLastUpdate());
          break;
        case FIELD_CAPACITY:
          long  dlong = d1.getCapacity() - d2.getCapacity();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_USED:
          dlong = d1.getDfsUsed() - d2.getDfsUsed();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_NONDFS_USED:
          dlong = d1.getNonDfsUsed() - d2.getNonDfsUsed();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_REMAINING:
          dlong = d1.getRemaining() - d2.getRemaining();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_PERCENT_USED:
          double ddbl =((d1.getDfsUsedPercent())-
                        (d2.getDfsUsedPercent()));
          ret = (ddbl < 0) ? -1 : ((ddbl > 0) ? 1 : 0);
          break;
        case FIELD_PERCENT_REMAINING:
          ddbl =((d1.getRemainingPercent())-
                 (d2.getRemainingPercent()));
          ret = (ddbl < 0) ? -1 : ((ddbl > 0) ? 1 : 0);
          break;
        case FIELD_BLOCKS:
          ret = d1.numBlocks() - d2.numBlocks();
          break;
        case FIELD_ADMIN_STATE:
          ret = d1.getAdminState().toString().compareTo(
              d2.getAdminState().toString());
          break;
        case FIELD_DECOMMISSIONED:
          ret = DFSUtil.DECOM_COMPARATOR.compare(d1, d2);
          break;
        case FIELD_NAME: 
          ret = d1.getHostName().compareTo(d2.getHostName());
          break;
        default:
          throw new IllegalArgumentException("Invalid sortField");
        }
        return (sortOrder == SORT_ORDER_DSC) ? -ret : ret;
      }
    }
        
    Collections.sort(nodes, new NodeComapare(field, order));
  }

  public static void printPathWithLinks(String dir, JspWriter out, 
                                        int namenodeInfoPort,
                                        String tokenString,
                                        String nnAddress
                                        ) throws IOException {
    try {
      String[] parts = dir.split(Path.SEPARATOR);
      StringBuilder tempPath = new StringBuilder(dir.length());
      out.print("<a href=\"browseDirectory.jsp" + "?dir="+ Path.SEPARATOR
          + "&namenodeInfoPort=" + namenodeInfoPort
          + getDelegationTokenUrlParam(tokenString) 
          + getUrlParam(NAMENODE_ADDRESS, nnAddress) + "\">" + Path.SEPARATOR
          + "</a>");
      tempPath.append(Path.SEPARATOR);
      for (int i = 0; i < parts.length-1; i++) {
        if (!parts[i].equals("")) {
          tempPath.append(parts[i]);
          out.print("<a href=\"browseDirectory.jsp" + "?dir="
              + HtmlQuoting.quoteHtmlChars(tempPath.toString()) + "&namenodeInfoPort=" + namenodeInfoPort
              + getDelegationTokenUrlParam(tokenString)
              + getUrlParam(NAMENODE_ADDRESS, nnAddress));
          out.print("\">" + HtmlQuoting.quoteHtmlChars(parts[i]) + "</a>" + Path.SEPARATOR);
          tempPath.append(Path.SEPARATOR);
        }
      }
      if(parts.length > 0) {
        out.print(HtmlQuoting.quoteHtmlChars(parts[parts.length-1]));
      }
    }
    catch (UnsupportedEncodingException ex) {
      ex.printStackTrace();
    }
  }

  public static void printGotoForm(JspWriter out,
                                   int namenodeInfoPort,
                                   String tokenString,
                                   String file,
                                   String nnAddress) throws IOException {
    out.print("<form action=\"browseDirectory.jsp\" method=\"get\" name=\"goto\">");
    out.print("Goto : ");
    out.print("<input name=\"dir\" type=\"text\" width=\"50\" id=\"dir\" value=\""+ HtmlQuoting.quoteHtmlChars(file)+"\"/>");
    out.print("<input name=\"go\" type=\"submit\" value=\"go\"/>");
    out.print("<input name=\"namenodeInfoPort\" type=\"hidden\" "
        + "value=\"" + namenodeInfoPort  + "\"/>");
    if (UserGroupInformation.isSecurityEnabled()) {
      out.print("<input name=\"" + DELEGATION_PARAMETER_NAME
          + "\" type=\"hidden\" value=\"" + tokenString + "\"/>");
    }
    out.print("<input name=\""+ NAMENODE_ADDRESS +"\" type=\"hidden\" "
        + "value=\"" + nnAddress  + "\"/>");
    out.print("</form>");
  }
  
  public static void createTitle(JspWriter out, 
                                 HttpServletRequest req, 
                                 String  file) throws IOException{
    if(file == null) file = "";
    int start = Math.max(0,file.length() - 100);
    if(start != 0)
      file = "..." + file.substring(start, file.length());
    out.print("<title>HDFS:" + file + "</title>");
  }

  /** Convert a String to chunk-size-to-view. */
  public static int string2ChunkSizeToView(String s, int defaultValue) {
    int n = s == null? 0: Integer.parseInt(s);
    return n > 0? n: defaultValue;
  }

  /** Return a table containing version information. */
  public static String getVersionTable() {
    return "<div class='dfstable'><table>"       
        + "\n  <tr><td class='col1'>Version:</td><td>" + VersionInfo.getVersion() + ", " + VersionInfo.getRevision() + "</td></tr>"
        + "\n  <tr><td class='col1'>Compiled:</td><td>" + VersionInfo.getDate() + " by " + VersionInfo.getUser() + " from " + VersionInfo.getBranch() + "</td></tr>"
        + "\n</table></div>";
  }

  /**
   * Validate filename. 
   * @return null if the filename is invalid.
   *         Otherwise, return the validated filename.
   */
  public static String validatePath(String p) {
    return p == null || p.length() == 0?
        null: new Path(p).toUri().getPath();
  }

  /**
   * Validate a long value. 
   * @return null if the value is invalid.
   *         Otherwise, return the validated Long object.
   */
  public static Long validateLong(String value) {
    return value == null? null: Long.parseLong(value);
  }

  /**
   * Validate a URL.
   * @return null if the value is invalid.
   *         Otherwise, return the validated URL String.
   */
  public static String validateURL(String value) {
    try {
      return URLEncoder.encode(new URL(value).toString(), "UTF-8");
    } catch (IOException e) {
      return null;
    }
  }
  
  /**
   * If security is turned off, what is the default web user?
   * @param conf the configuration to look in
   * @return the remote user that was configuration
   */
  public static UserGroupInformation getDefaultWebUser(Configuration conf
                                                       ) throws IOException {
    return UserGroupInformation.createRemoteUser(getDefaultWebUserName(conf));
  }

  private static String getDefaultWebUserName(Configuration conf
      ) throws IOException {
    String user = conf.get(
        HADOOP_HTTP_STATIC_USER, DEFAULT_HADOOP_HTTP_STATIC_USER);
    if (user == null || user.length() == 0) {
      throw new IOException("Cannot determine UGI from request or conf");
    }
    return user;
  }

  private static InetSocketAddress getNNServiceAddress(ServletContext context,
      HttpServletRequest request) {
    String namenodeAddressInUrl = request.getParameter(NAMENODE_ADDRESS);
    InetSocketAddress namenodeAddress = null;
    if (namenodeAddressInUrl != null) {
      namenodeAddress = NetUtils.createSocketAddr(namenodeAddressInUrl);
    } else if (context != null) {
      namenodeAddress = NameNodeHttpServer.getNameNodeAddressFromContext(
          context); 
    }
    if (namenodeAddress != null) {
      return namenodeAddress;
    }
    return null;
  }

  /** Same as getUGI(null, request, conf). */
  public static UserGroupInformation getUGI(HttpServletRequest request,
      Configuration conf) throws IOException {
    return getUGI(null, request, conf);
  }
  
  /** Same as getUGI(context, request, conf, KERBEROS_SSL, true). */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf) throws IOException {
    return getUGI(context, request, conf, AuthenticationMethod.KERBEROS_SSL, true);
  }

  /**
   * Get {@link UserGroupInformation} and possibly the delegation token out of
   * the request.
   * @param context the ServletContext that is serving this request.
   * @param request the http request
   * @param conf configuration
   * @param secureAuthMethod the AuthenticationMethod used in secure mode.
   * @param tryUgiParameter Should it try the ugi parameter?
   * @return a new user from the request
   * @throws AccessControlException if the request has no token
   */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf,
      final AuthenticationMethod secureAuthMethod,
      final boolean tryUgiParameter) throws IOException {
    UserGroupInformation ugi = null;
    final String usernameFromQuery = getUsernameFromQuery(request, tryUgiParameter);
    final String doAsUserFromQuery = request.getParameter(DoAsParam.NAME);
    final String remoteUser;
   
    if (UserGroupInformation.isSecurityEnabled()) {
      remoteUser = request.getRemoteUser();
      final String tokenString = request.getParameter(DELEGATION_PARAMETER_NAME);
      if (tokenString != null) {
        // Token-based connections need only verify the effective user, and
        // disallow proxying to different user.  Proxy authorization checks
        // are not required since the checks apply to issuing a token.
        ugi = getTokenUGI(context, request, tokenString, conf);
        checkUsername(ugi.getShortUserName(), usernameFromQuery);
        checkUsername(ugi.getShortUserName(), doAsUserFromQuery);
      } else if (remoteUser == null) {
        throw new IOException(
            "Security enabled but user not authenticated by filter");
      }
    } else {
      // Security's not on, pull from url or use default web user
      remoteUser = (usernameFromQuery == null)
          ? getDefaultWebUserName(conf) // not specified in request
          : usernameFromQuery;
    }

    if (ugi == null) { // security is off, or there's no token
      ugi = UserGroupInformation.createRemoteUser(remoteUser);
      checkUsername(ugi.getShortUserName(), usernameFromQuery);
      if (UserGroupInformation.isSecurityEnabled()) {
        // This is not necessarily true, could have been auth'ed by user-facing
        // filter
        ugi.setAuthenticationMethod(secureAuthMethod);
      }
      if (doAsUserFromQuery != null) {
        // create and attempt to authorize a proxy user
        ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);
        ProxyUsers.authorize(ugi, getRemoteAddr(request));
      }
    }
    
    if(LOG.isDebugEnabled())
      LOG.debug("getUGI is returning: " + ugi.getShortUserName());
    return ugi;
  }

  private static UserGroupInformation getTokenUGI(ServletContext context,
                                                  HttpServletRequest request,
                                                  String tokenString,
                                                  Configuration conf)
                                                      throws IOException {
    final Token<DelegationTokenIdentifier> token =
        new Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(tokenString);
    InetSocketAddress serviceAddress = getNNServiceAddress(context, request);
    if (serviceAddress != null) {
      SecurityUtil.setTokenService(token, serviceAddress);
      token.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
    }

    ByteArrayInputStream buf =
        new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    id.readFields(in);
    if (context != null) {
      final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
      if (nn != null) {
        // Verify the token.
        nn.getNamesystem().verifyToken(id, token.getPassword());
      }
    }
    UserGroupInformation ugi = id.getUser();
    ugi.addToken(token);
    return ugi;
  }

  // honor the X-Forwarded-For header set by a configured set of trusted
  // proxy servers.  allows audit logging and proxy user checks to work
  // via an http proxy
  public static String getRemoteAddr(HttpServletRequest request) {
    String remoteAddr = request.getRemoteAddr();
    String proxyHeader = request.getHeader("X-Forwarded-For");
    if (proxyHeader != null && ProxyServers.isProxyServer(remoteAddr)) {
      final String clientAddr = proxyHeader.split(",")[0].trim();
      if (!clientAddr.isEmpty()) {
        remoteAddr = clientAddr;
      }
    }
    return remoteAddr;
  }


  /**
   * Expected user name should be a short name.
   */
  private static void checkUsername(final String expected, final String name
      ) throws IOException {
    if (expected == null && name != null) {
      throw new IOException("Usernames not matched: expecting null but name="
          + name);
    }
    if (name == null) { //name is optional, null is okay
      return;
    }
    KerberosName u = new KerberosName(name);
    String shortName = u.getShortName();
    if (!shortName.equals(expected)) {
      throw new IOException("Usernames not matched: name=" + shortName
          + " != expected=" + expected);
    }
  }

  private static String getUsernameFromQuery(final HttpServletRequest request,
      final boolean tryUgiParameter) {
    String username = request.getParameter(UserParam.NAME);
    if (username == null && tryUgiParameter) {
      //try ugi parameter
      final String ugiStr = request.getParameter("ugi");
      if (ugiStr != null) {
        username = ugiStr.split(",")[0];
      }
    }
    return username;
  }

  /**
   * Returns the url parameter for the given token string.
   * @param tokenString
   * @return url parameter
   */
  public static String getDelegationTokenUrlParam(String tokenString) {
    if (tokenString == null ) {
      return "";
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      return SET_DELEGATION + tokenString;
    } else {
      return "";
    }
  }

  /**
   * Returns the url parameter for the given string, prefixed with
   * paramSeparator.
   * 
   * @param name parameter name
   * @param val parameter value
   * @param paramSeparator URL parameter prefix, i.e. either '?' or '&'
   * @return url parameter
   */
  public static String getUrlParam(String name, String val, String paramSeparator) {
    return val == null ? "" : paramSeparator + name + "=" + val;
  }
  
  /**
   * Returns the url parameter for the given string, prefixed with '?' if
   * firstParam is true, prefixed with '&' if firstParam is false.
   * 
   * @param name parameter name
   * @param val parameter value
   * @param firstParam true if this is the first parameter in the list, false otherwise
   * @return url parameter
   */
  public static String getUrlParam(String name, String val, boolean firstParam) {
    return getUrlParam(name, val, firstParam ? "?" : "&");
  }
  
  /**
   * Returns the url parameter for the given string, prefixed with '&'.
   * 
   * @param name parameter name
   * @param val parameter value
   * @return url parameter
   */
  public static String getUrlParam(String name, String val) {
    return getUrlParam(name, val, false);
  }
}
