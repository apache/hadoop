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

package org.apache.hadoop.hdfs.web;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
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
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
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
    implements DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator {
  public static final String SCHEME = "hftp";

  static {
    HttpURLConnection.setFollowRedirects(true);
  }

  URLConnectionFactory connectionFactory;

  public static final Text TOKEN_KIND = new Text("HFTP delegation");

  protected UserGroupInformation ugi;
  private URI hftpURI;

  protected URI nnUri;

  public static final String HFTP_TIMEZONE = "UTC";
  public static final String HFTP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

  protected TokenAspect<? extends HftpFileSystem> tokenAspect;
  private Token<?> delegationToken;
  private Token<?> renewToken;
  protected Text tokenServiceName;

  @Override
  public URI getCanonicalUri() {
    return super.getCanonicalUri();
  }

  public static final SimpleDateFormat getDateFormat() {
    final SimpleDateFormat df = new SimpleDateFormat(HFTP_DATE_FORMAT);
    df.setTimeZone(TimeZone.getTimeZone(HFTP_TIMEZONE));
    return df;
  }

  protected static final ThreadLocal<SimpleDateFormat> df =
    new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return getDateFormat();
    }
  };

  @Override
  protected int getDefaultPort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  /**
   *  We generate the address with one of the following ports, in
   *  order of preference.
   *  1. Port from the hftp URI e.g. hftp://namenode:4000/ will return 4000.
   *  2. Port configured via DFS_NAMENODE_HTTP_PORT_KEY
   *  3. DFS_NAMENODE_HTTP_PORT_DEFAULT i.e. 50070.
   *
   * @param uri
   */
  protected InetSocketAddress getNamenodeAddr(URI uri) {
    // use authority so user supplied uri can override port
    return NetUtils.createSocketAddr(uri.getAuthority(), getDefaultPort());
  }

  protected URI getNamenodeUri(URI uri) {
    return DFSUtil.createUri(getUnderlyingProtocol(), getNamenodeAddr(uri));
  }

  /**
   * See the documentation of {@Link #getNamenodeAddr(URI)} for the logic
   * behind selecting the canonical service name.
   * @return
   */
  @Override
  public String getCanonicalServiceName() {
    return SecurityUtil.buildTokenService(nnUri).toString();
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return NetUtils.getCanonicalUri(uri, getDefaultPort());
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>hftp</code>
   */
  @Override
  public String getScheme() {
    return SCHEME;
  }

  /**
   * Initialize connectionFactory and tokenAspect. This function is intended to
   * be overridden by HsFtpFileSystem.
   */
  protected void initTokenAspect() {
    tokenAspect = new TokenAspect<HftpFileSystem>(this, tokenServiceName, TOKEN_KIND);
  }

  @Override
  public void initialize(final URI name, final Configuration conf)
  throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    this.connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);
    this.ugi = UserGroupInformation.getCurrentUser();
    this.nnUri = getNamenodeUri(name);
    this.tokenServiceName = SecurityUtil.buildTokenService(nnUri);

    try {
      this.hftpURI = new URI(name.getScheme(), name.getAuthority(),
                             null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }

    initTokenAspect();
    if (UserGroupInformation.isSecurityEnabled()) {
      tokenAspect.initDelegationToken(ugi);
    }
  }

  @Override
  public Token<?> getRenewToken() {
    return renewToken;
  }

  /**
   * Return the underlying protocol that is used to talk to the namenode.
   */
  protected String getUnderlyingProtocol() {
    return "http";
  }

  @Override
  public synchronized <T extends TokenIdentifier> void setDelegationToken(Token<T> token) {
    /**
     * XXX The kind of the token has been changed by DelegationTokenFetcher. We
     * use the token for renewal, since the reflection utilities needs the value
     * of the kind field to correctly renew the token.
     *
     * For other operations, however, the client has to send a
     * HDFS_DELEGATION_KIND token over the wire so that it can talk to Hadoop
     * 0.20.203 clusters. Later releases fix this problem. See HDFS-5440 for
     * more details.
     */
    renewToken = token;
    delegationToken = new Token<T>(token);
    delegationToken.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
  }

  @Override
  public synchronized Token<?> getDelegationToken(final String renewer)
      throws IOException {
    try {
      // Renew TGT if needed
      UserGroupInformation connectUgi = ugi.getRealUser();
      final String proxyUser = connectUgi == null ? null : ugi
          .getShortUserName();
      if (connectUgi == null) {
        connectUgi = ugi;
      }
      return connectUgi.doAs(new PrivilegedExceptionAction<Token<?>>() {
        @Override
        public Token<?> run() throws IOException {
          Credentials c;
          try {
            c = DelegationTokenFetcher.getDTfromRemote(connectionFactory,
                nnUri, renewer, proxyUser);
          } catch (IOException e) {
            if (e.getCause() instanceof ConnectException) {
              LOG.warn("Couldn't connect to " + nnUri +
                  ", assuming security is disabled");
              return null;
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Exception getting delegation token", e);
            }
            throw e;
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
    return hftpURI;
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
    final URL url = new URL(getUnderlyingProtocol(), nnUri.getHost(),
          nnUri.getPort(), path + '?' + query);
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
    StringBuilder ugiParameter = new StringBuilder(
        ServletUtil.encodeQueryValue(ugi.getShortUserName()));
    for(String g: ugi.getGroupNames()) {
      ugiParameter.append(",");
      ugiParameter.append(ServletUtil.encodeQueryValue(g));
    }
    return ugiParameter.toString();
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
    final HttpURLConnection connection;
    connection = (HttpURLConnection)connectionFactory.openConnection(url);
    connection.setRequestMethod("GET");
    connection.connect();
    return connection;
  }

  protected String addDelegationTokenParam(String query) throws IOException {
    String tokenString = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      synchronized (this) {
        tokenAspect.ensureTokenInitialized();
        if (delegationToken != null) {
          tokenString = delegationToken.encodeToUrlString();
          return (query + JspHelper.getDelegationTokenUrlParam(tokenString));
        }
      }
    }
    return query;
  }

  static class RangeHeaderUrlOpener extends ByteRangeInputStream.URLOpener {
    private final URLConnectionFactory connFactory;

    RangeHeaderUrlOpener(URLConnectionFactory connFactory, final URL url) {
      super(url);
      this.connFactory = connFactory;
    }

    protected HttpURLConnection openConnection() throws IOException {
      return (HttpURLConnection)connFactory.openConnection(url);
    }

    /** Use HTTP Range header for specifying offset. */
    @Override
    protected HttpURLConnection connect(final long offset,
        final boolean resolved) throws IOException {
      final HttpURLConnection conn = openConnection();
      conn.setRequestMethod("GET");
      if (offset != 0L) {
        conn.setRequestProperty("Range", "bytes=" + offset + "-");
      }
      conn.connect();

      //Expects HTTP_OK or HTTP_PARTIAL response codes.
      final int code = conn.getResponseCode();
      if (offset != 0L && code != HttpURLConnection.HTTP_PARTIAL) {
        throw new IOException("HTTP_PARTIAL expected, received " + code);
      } else if (offset == 0L && code != HttpURLConnection.HTTP_OK) {
        throw new IOException("HTTP_OK expected, received " + code);
      }
      return conn;
    }
  }

  static class RangeHeaderInputStream extends ByteRangeInputStream {
    RangeHeaderInputStream(RangeHeaderUrlOpener o, RangeHeaderUrlOpener r)
    throws IOException {
      super(o, r);
    }

    RangeHeaderInputStream(URLConnectionFactory connFactory, final URL url)
    throws IOException {
      this(new RangeHeaderUrlOpener(connFactory, url),
          new RangeHeaderUrlOpener(connFactory, null));
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
    return new FSDataInputStream(new RangeHeaderInputStream(connectionFactory, u));
  }

  @Override
  public void close() throws IOException {
    super.close();
    tokenAspect.removeRenewAction();
  }

  /** Class to parse and store a listing reply from the server. */
  class LsParser extends DefaultHandler {

    final ArrayList<FileStatus> fslist = new ArrayList<FileStatus>();

    @Override
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
              Long.parseLong(attrs.getValue("size")), false,
              Short.valueOf(attrs.getValue("replication")).shortValue(),
              Long.parseLong(attrs.getValue("blocksize")),
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              HftpFileSystem.this.makeQualified(
                  new Path(getUri().toString(), ServletUtil.decodePath(
                      attrs.getValue("path")))))
        : new FileStatus(0L, true, 0, 0L,
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              HftpFileSystem.this.makeQualified(
                  new Path(getUri().toString(), ServletUtil.decodePath(
                      attrs.getValue("path")))));
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

    @Override
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

  @Override
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
  @Override
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

    @Override
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

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    final String s = makeQualified(f).toUri().getPath();
    final ContentSummary cs = new ContentSummaryParser().getContentSummary(s);
    return cs != null? cs: super.getContentSummary(f);
  }

  @SuppressWarnings("unchecked")
  @Override
  public long renewDelegationToken(final Token<?> token) throws IOException {
    // update the kerberos credentials, if they are coming from a keytab
    UserGroupInformation connectUgi = ugi.getRealUser();
    if (connectUgi == null) {
      connectUgi = ugi;
    }
    try {
      return connectUgi.doAs(new PrivilegedExceptionAction<Long>() {
        @Override
        public Long run() throws Exception {
          InetSocketAddress serviceAddr = SecurityUtil
              .getTokenServiceAddr(token);
          return DelegationTokenFetcher.renewDelegationToken(connectionFactory,
              DFSUtil.createUri(getUnderlyingProtocol(), serviceAddr),
              (Token<DelegationTokenIdentifier>) token);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void cancelDelegationToken(final Token<?> token) throws IOException {
    UserGroupInformation connectUgi = ugi.getRealUser();
    if (connectUgi == null) {
      connectUgi = ugi;
    }
    try {
      connectUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          InetSocketAddress serviceAddr = SecurityUtil
              .getTokenServiceAddr(token);
          DelegationTokenFetcher.cancelDelegationToken(connectionFactory,
              DFSUtil.createUri(getUnderlyingProtocol(), serviceAddr),
              (Token<DelegationTokenIdentifier>) token);
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
