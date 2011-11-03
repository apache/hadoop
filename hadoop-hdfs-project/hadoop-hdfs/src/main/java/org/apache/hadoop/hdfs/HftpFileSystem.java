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

package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenRenewer;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ServletUtil;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * An implementation of a protocol for accessing filesystems over HTTP.
 * The following implementation provides a limited, read-only interface
 * to a filesystem over HTTP.
 * @see org.apache.hadoop.hdfs.server.namenode.ListPathsServlet
 * @see org.apache.hadoop.hdfs.server.namenode.FileDataServlet
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HftpFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable {
  private static final DelegationTokenRenewer<HftpFileSystem> dtRenewer
      = new DelegationTokenRenewer<HftpFileSystem>(HftpFileSystem.class);
  
  static {
    HttpURLConnection.setFollowRedirects(true);
    dtRenewer.start();
  }

  public static final Text TOKEN_KIND = new Text("HFTP delegation");

  private String nnHttpUrl;
  private Text hdfsServiceName;
  private URI hftpURI;
  protected InetSocketAddress nnAddr;
  protected UserGroupInformation ugi; 

  public static final String HFTP_TIMEZONE = "UTC";
  public static final String HFTP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
  private Token<?> delegationToken;
  private Token<?> renewToken;
  
  public static final SimpleDateFormat getDateFormat() {
    final SimpleDateFormat df = new SimpleDateFormat(HFTP_DATE_FORMAT);
    df.setTimeZone(TimeZone.getTimeZone(HFTP_TIMEZONE));
    return df;
  }

  protected static final ThreadLocal<SimpleDateFormat> df =
    new ThreadLocal<SimpleDateFormat>() {
    protected SimpleDateFormat initialValue() {
      return getDateFormat();
    }
  };

  @Override
  protected int getDefaultPort() {
    return getDefaultSecurePort();

    //TODO: un-comment the following once HDFS-7510 is committed. 
//    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
//        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  protected int getDefaultSecurePort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT);
  }

  @Override
  public String getCanonicalServiceName() {
    return SecurityUtil.buildDTServiceName(hftpURI, getDefaultPort());
  }
  
  private String buildUri(String schema, String host, int port) {
    StringBuilder sb = new StringBuilder(schema);
    return sb.append(host).append(":").append(port).toString();
  }


  @Override
  public void initialize(final URI name, final Configuration conf)
  throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    this.ugi = UserGroupInformation.getCurrentUser(); 
    nnAddr = NetUtils.createSocketAddr(name.toString());
    
    // in case we open connection to hftp of a different cluster
    // we need to know this cluster https port
    // if it is not set we assume it is the same cluster or same port
    int urlPort = conf.getInt("dfs.hftp.https.port", -1);
    if(urlPort == -1)
      urlPort = conf.getInt(DFSConfigKeys.DFS_HTTPS_PORT_KEY, 
          DFSConfigKeys.DFS_HTTPS_PORT_DEFAULT);

    String normalizedNN = NetUtils.normalizeHostName(name.getHost());
    nnHttpUrl = buildUri("https://", normalizedNN ,urlPort);
    LOG.debug("using url to get DT:" + nnHttpUrl);
    try {
      hftpURI = new URI(buildUri("hftp://", normalizedNN, urlPort));
    } catch (URISyntaxException ue) {
      throw new IOException("bad uri for hdfs", ue);
    }

    // if one uses RPC port different from the Default one,  
    // one should specify what is the setvice name for this delegation token
    // otherwise it is hostname:RPC_PORT
    String key = DelegationTokenSelector.SERVICE_NAME_KEY
        + SecurityUtil.buildDTServiceName(name,
            DFSConfigKeys.DFS_HTTPS_PORT_DEFAULT);
    if(LOG.isDebugEnabled()) {
      LOG.debug("Trying to find DT for " + name + " using key=" + key + 
          "; conf=" + conf.get(key, ""));
    }
    String nnServiceName = conf.get(key);
    int nnPort = NameNode.DEFAULT_PORT;
    if (nnServiceName != null) { // get the real port
      nnPort = NetUtils.createSocketAddr(nnServiceName, 
          NameNode.DEFAULT_PORT).getPort();
    }
    try {
      URI hdfsURI = new URI("hdfs://" + normalizedNN + ":" + nnPort);
      hdfsServiceName = new Text(SecurityUtil.buildDTServiceName(hdfsURI, 
                                                                 nnPort));
    } catch (URISyntaxException ue) {
      throw new IOException("bad uri for hdfs", ue);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      //try finding a token for this namenode (esp applicable for tasks
      //using hftp). If there exists one, just set the delegationField
      String hftpServiceName = getCanonicalServiceName();
      for (Token<? extends TokenIdentifier> t : ugi.getTokens()) {
        Text kind = t.getKind();
        if (DelegationTokenIdentifier.HDFS_DELEGATION_KIND.equals(kind)) {
          if (t.getService().equals(hdfsServiceName)) {
            setDelegationToken(t);
            break;
          }
        } else if (TOKEN_KIND.equals(kind)) {
          if (hftpServiceName
              .equals(normalizeService(t.getService().toString()))) {
            setDelegationToken(t);
            break;
          }
        }
      }
      
      //since we don't already have a token, go get one over https
      if (delegationToken == null) {
        setDelegationToken(getDelegationToken(null));
        dtRenewer.addRenewAction(this);
      }
    }
  }

  private String normalizeService(String service) {
    int colonIndex = service.indexOf(':');
    if (colonIndex == -1) {
      throw new IllegalArgumentException("Invalid service for hftp token: " + 
                                         service);
    }
    String hostname = 
        NetUtils.normalizeHostName(service.substring(0, colonIndex));
    String port = service.substring(colonIndex + 1);
    return hostname + ":" + port;
  }

  //TODO: un-comment the following once HDFS-7510 is committed. 
//  protected Token<DelegationTokenIdentifier> selectHftpDelegationToken() {
//    Text serviceName = SecurityUtil.buildTokenService(nnSecureAddr);
//    return hftpTokenSelector.selectToken(serviceName, ugi.getTokens());      
//  }
  
  protected Token<DelegationTokenIdentifier> selectHdfsDelegationToken() {
    return  DelegationTokenSelector.selectHdfsDelegationToken(
        nnAddr, ugi, getConf());
  }
  

  @Override
  public Token<?> getRenewToken() {
    return renewToken;
  }

  @Override
  public <T extends TokenIdentifier> void setDelegationToken(Token<T> token) {
    renewToken = token;
    // emulate the 203 usage of the tokens
    // by setting the kind and service as if they were hdfs tokens
    delegationToken = new Token<T>(token);
    delegationToken.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
    delegationToken.setService(hdfsServiceName);
  }

  @Override
  public synchronized Token<?> getDelegationToken(final String renewer
                                                  ) throws IOException {
    try {
      //Renew TGT if needed
      ugi.reloginFromKeytab();
      return ugi.doAs(new PrivilegedExceptionAction<Token<?>>() {
        public Token<?> run() throws IOException {
          Credentials c;
          try {
            c = DelegationTokenFetcher.getDTfromRemote(nnHttpUrl, renewer);
          } catch (Exception e) {
            LOG.info("Couldn't get a delegation token from " + nnHttpUrl + 
            " using https.");
            if(LOG.isDebugEnabled()) {
              LOG.debug("error was ", e);
            }
            //Maybe the server is in unsecure mode (that's bad but okay)
            return null;
          }
          for (Token<? extends TokenIdentifier> t : c.getAllTokens()) {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Got dt for " + getUri() + ";t.service="
                  +t.getService());
            }
            return t;
          }
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public URI getUri() {
    try {
      return new URI("hftp", null, nnAddr.getHostName(), nnAddr.getPort(),
                     null, null, null);
    } catch (URISyntaxException e) {
      return null;
    } 
  }

  /**
   * Return a URL pointing to given path on the namenode.
   *
   * @param path to obtain the URL for
   * @param query string to append to the path
   * @return namenode URL referring to the given path
   * @throws IOException on error constructing the URL
   */
  protected URL getNamenodeURL(String path, String query) throws IOException {
    final URL url = new URL("http", nnAddr.getHostName(),
          nnAddr.getPort(), path + '?' + query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }

  /**
   * Get encoded UGI parameter string for a URL.
   * 
   * @return user_shortname,group1,group2...
   */
  private String getEncodedUgiParameter() {
    StringBuilder ugiParamenter = new StringBuilder(
        ServletUtil.encodeQueryValue(ugi.getShortUserName()));
    for(String g: ugi.getGroupNames()) {
      ugiParamenter.append(",");
      ugiParamenter.append(ServletUtil.encodeQueryValue(g));
    }
    return ugiParamenter.toString();
  }
  
  static Void throwIOExceptionFromConnection(
      final HttpURLConnection connection, final IOException ioe
      ) throws IOException {
    final int code = connection.getResponseCode();
    final String s = connection.getResponseMessage();
    throw s == null? ioe:
        new IOException(s + " (error code=" + code + ")", ioe);
  }

  /**
   * Open an HTTP connection to the namenode to read file data and metadata.
   * @param path The path component of the URL
   * @param query The query component of the URL
   */
  protected HttpURLConnection openConnection(String path, String query)
      throws IOException {
    query = addDelegationTokenParam(query);
    final URL url = getNamenodeURL(path, query);
    final HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    try {
      connection.setRequestMethod("GET");
      connection.connect();
    } catch (IOException ioe) {
      throwIOExceptionFromConnection(connection, ioe);
    }
    return connection;
  }

  protected String addDelegationTokenParam(String query) throws IOException {
    String tokenString = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      synchronized (this) {
        if (delegationToken != null) {
          tokenString = delegationToken.encodeToUrlString();
          return (query + JspHelper.getDelegationTokenUrlParam(tokenString));
        }
      }
    }
    return query;
  }

  static class RangeHeaderUrlOpener extends ByteRangeInputStream.URLOpener {
    RangeHeaderUrlOpener(final URL url) {
      super(url);
    }

    @Override
    protected HttpURLConnection openConnection() throws IOException {
      return (HttpURLConnection)url.openConnection();
    }

    /** Use HTTP Range header for specifying offset. */
    @Override
    protected HttpURLConnection openConnection(final long offset) throws IOException {
      final HttpURLConnection conn = openConnection();
      conn.setRequestMethod("GET");
      if (offset != 0L) {
        conn.setRequestProperty("Range", "bytes=" + offset + "-");
      }
      return conn;
    }  
  }

  static class RangeHeaderInputStream extends ByteRangeInputStream {
    RangeHeaderInputStream(RangeHeaderUrlOpener o, RangeHeaderUrlOpener r) {
      super(o, r);
    }

    RangeHeaderInputStream(final URL url) {
      this(new RangeHeaderUrlOpener(url), new RangeHeaderUrlOpener(null));
    }

    /** Expects HTTP_OK and HTTP_PARTIAL response codes. */
    @Override
    protected void checkResponseCode(final HttpURLConnection connection
        ) throws IOException {
      final int code = connection.getResponseCode();
      if (startPos != 0 && code != HttpURLConnection.HTTP_PARTIAL) {
        // We asked for a byte range but did not receive a partial content
        // response...
        throw new IOException("HTTP_PARTIAL expected, received " + code);
      } else if (startPos == 0 && code != HttpURLConnection.HTTP_OK) {
        // We asked for all bytes from the beginning but didn't receive a 200
        // response (none of the other 2xx codes are valid here)
        throw new IOException("HTTP_OK expected, received " + code);
      }
    }

    @Override
    protected URL getResolvedUrl(final HttpURLConnection connection) {
      return connection.getURL();
    }
  }

  @Override
  public FSDataInputStream open(Path f, int buffersize) throws IOException {
    f = f.makeQualified(getUri(), getWorkingDirectory());
    String path = "/data" + ServletUtil.encodePath(f.toUri().getPath());
    String query = addDelegationTokenParam("ugi=" + getEncodedUgiParameter());
    URL u = getNamenodeURL(path, query);    
    return new FSDataInputStream(new RangeHeaderInputStream(u));
  }

  /** Class to parse and store a listing reply from the server. */
  class LsParser extends DefaultHandler {

    ArrayList<FileStatus> fslist = new ArrayList<FileStatus>();

    public void startElement(String ns, String localname, String qname,
                Attributes attrs) throws SAXException {
      if ("listing".equals(qname)) return;
      if (!"file".equals(qname) && !"directory".equals(qname)) {
        if (RemoteException.class.getSimpleName().equals(qname)) {
          throw new SAXException(RemoteException.valueOf(attrs));
        }
        throw new SAXException("Unrecognized entry: " + qname);
      }
      long modif;
      long atime = 0;
      try {
        final SimpleDateFormat ldf = df.get();
        modif = ldf.parse(attrs.getValue("modified")).getTime();
        String astr = attrs.getValue("accesstime");
        if (astr != null) {
          atime = ldf.parse(astr).getTime();
        }
      } catch (ParseException e) { throw new SAXException(e); }
      FileStatus fs = "file".equals(qname)
        ? new FileStatus(
              Long.valueOf(attrs.getValue("size")).longValue(), false,
              Short.valueOf(attrs.getValue("replication")).shortValue(),
              Long.valueOf(attrs.getValue("blocksize")).longValue(),
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              HftpFileSystem.this.makeQualified(
                  new Path(getUri().toString(), attrs.getValue("path"))))
        : new FileStatus(0L, true, 0, 0L,
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              HftpFileSystem.this.makeQualified(
                  new Path(getUri().toString(), attrs.getValue("path"))));
      fslist.add(fs);
    }

    private void fetchList(String path, boolean recur) throws IOException {
      try {
        XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        HttpURLConnection connection = openConnection(
            "/listPaths" + ServletUtil.encodePath(path),
            "ugi=" + getEncodedUgiParameter() + (recur ? "&recursive=yes" : ""));
        InputStream resp = connection.getInputStream();
        xr.parse(new InputSource(resp));
      } catch(SAXException e) {
        final Exception embedded = e.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException)embedded;
        }
        throw new IOException("invalid xml directory content", e);
      }
    }

    public FileStatus getFileStatus(Path f) throws IOException {
      fetchList(f.toUri().getPath(), false);
      if (fslist.size() == 0) {
        throw new FileNotFoundException("File does not exist: " + f);
      }
      return fslist.get(0);
    }

    public FileStatus[] listStatus(Path f, boolean recur) throws IOException {
      fetchList(f.toUri().getPath(), recur);
      if (fslist.size() > 0 && (fslist.size() != 1 || fslist.get(0).isDirectory())) {
        fslist.remove(0);
      }
      return fslist.toArray(new FileStatus[0]);
    }

    public FileStatus[] listStatus(Path f) throws IOException {
      return listStatus(f, false);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    LsParser lsparser = new LsParser();
    return lsparser.listStatus(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LsParser lsparser = new LsParser();
    return lsparser.getFileStatus(f);
  }

  private class ChecksumParser extends DefaultHandler {
    private FileChecksum filechecksum;

    /** {@inheritDoc} */
    public void startElement(String ns, String localname, String qname,
                Attributes attrs) throws SAXException {
      if (!MD5MD5CRC32FileChecksum.class.getName().equals(qname)) {
        if (RemoteException.class.getSimpleName().equals(qname)) {
          throw new SAXException(RemoteException.valueOf(attrs));
        }
        throw new SAXException("Unrecognized entry: " + qname);
      }

      filechecksum = MD5MD5CRC32FileChecksum.valueOf(attrs);
    }

    private FileChecksum getFileChecksum(String f) throws IOException {
      final HttpURLConnection connection = openConnection(
          "/fileChecksum" + ServletUtil.encodePath(f), 
          "ugi=" + getEncodedUgiParameter());
      try {
        final XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        xr.parse(new InputSource(connection.getInputStream()));
      } catch(SAXException e) {
        final Exception embedded = e.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException)embedded;
        }
        throw new IOException("invalid xml directory content", e);
      } finally {
        connection.disconnect();
      }
      return filechecksum;
    }
  }

  /** {@inheritDoc} */
  public FileChecksum getFileChecksum(Path f) throws IOException {
    final String s = makeQualified(f).toUri().getPath();
    return new ChecksumParser().getFileChecksum(s);
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("/").makeQualified(getUri(), null);
  }

  @Override
  public void setWorkingDirectory(Path f) { }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new IOException("Not supported");
  }

  @Override 
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new IOException("Not supported");
  }
  
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * A parser for parsing {@link ContentSummary} xml.
   */
  private class ContentSummaryParser extends DefaultHandler {
    private ContentSummary contentsummary;

    /** {@inheritDoc} */
    public void startElement(String ns, String localname, String qname,
                Attributes attrs) throws SAXException {
      if (!ContentSummary.class.getName().equals(qname)) {
        if (RemoteException.class.getSimpleName().equals(qname)) {
          throw new SAXException(RemoteException.valueOf(attrs));
        }
        throw new SAXException("Unrecognized entry: " + qname);
      }

      contentsummary = toContentSummary(attrs);
    }

    /**
     * Connect to the name node and get content summary.  
     * @param path The path
     * @return The content summary for the path.
     * @throws IOException
     */
    private ContentSummary getContentSummary(String path) throws IOException {
      final HttpURLConnection connection = openConnection(
          "/contentSummary" + ServletUtil.encodePath(path), 
          "ugi=" + getEncodedUgiParameter());
      InputStream in = null;
      try {
        in = connection.getInputStream();        

        final XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        xr.parse(new InputSource(in));
      } catch(FileNotFoundException fnfe) {
        //the server may not support getContentSummary
        return null;
      } catch(SAXException saxe) {
        final Exception embedded = saxe.getException();
        if (embedded != null && embedded instanceof IOException) {
          throw (IOException)embedded;
        }
        throw new IOException("Invalid xml format", saxe);
      } finally {
        if (in != null) {
          in.close();
        }
        connection.disconnect();
      }
      return contentsummary;
    }
  }

  /** Return the object represented in the attributes. */
  private static ContentSummary toContentSummary(Attributes attrs
      ) throws SAXException {
    final String length = attrs.getValue("length");
    final String fileCount = attrs.getValue("fileCount");
    final String directoryCount = attrs.getValue("directoryCount");
    final String quota = attrs.getValue("quota");
    final String spaceConsumed = attrs.getValue("spaceConsumed");
    final String spaceQuota = attrs.getValue("spaceQuota");

    if (length == null
        || fileCount == null
        || directoryCount == null
        || quota == null
        || spaceConsumed == null
        || spaceQuota == null) {
      return null;
    }

    try {
      return new ContentSummary(
          Long.parseLong(length),
          Long.parseLong(fileCount),
          Long.parseLong(directoryCount),
          Long.parseLong(quota),
          Long.parseLong(spaceConsumed),
          Long.parseLong(spaceQuota));
    } catch(Exception e) {
      throw new SAXException("Invalid attributes: length=" + length
          + ", fileCount=" + fileCount
          + ", directoryCount=" + directoryCount
          + ", quota=" + quota
          + ", spaceConsumed=" + spaceConsumed
          + ", spaceQuota=" + spaceQuota, e);
    }
  }

  /** {@inheritDoc} */
  public ContentSummary getContentSummary(Path f) throws IOException {
    final String s = makeQualified(f).toUri().getPath();
    final ContentSummary cs = new ContentSummaryParser().getContentSummary(s);
    return cs != null? cs: super.getContentSummary(f);
  }

  @InterfaceAudience.Private
  public static class TokenManager extends TokenRenewer {

    @Override
    public boolean handleKind(Text kind) {
      return kind.equals(TOKEN_KIND);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public long renew(Token<?> token, 
                      Configuration conf) throws IOException {
      // update the kerberos credentials, if they are coming from a keytab
      UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      // use https to renew the token
      return 
        DelegationTokenFetcher.renewDelegationToken
        ("https://" + token.getService().toString(), 
         (Token<DelegationTokenIdentifier>) token);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cancel(Token<?> token, 
                       Configuration conf) throws IOException {
      // update the kerberos credentials, if they are coming from a keytab
      UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      // use https to cancel the token
      DelegationTokenFetcher.cancelDelegationToken
        ("https://" + token.getService().toString(), 
         (Token<DelegationTokenIdentifier>) token);
    }
    
  }
}
