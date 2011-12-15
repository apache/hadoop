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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenRenewer;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
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
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.apache.hadoop.util.Progressable;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/** An implementation of a protocol for accessing filesystems over HTTP.
 * The following implementation provides a limited, read-only interface
 * to a filesystem over HTTP.
 * @see org.apache.hadoop.hdfs.server.namenode.ListPathsServlet
 * @see org.apache.hadoop.hdfs.server.namenode.FileDataServlet
 */
public class HftpFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable {
  private static final DelegationTokenRenewer<HftpFileSystem> dtRenewer
      = new DelegationTokenRenewer<HftpFileSystem>(HftpFileSystem.class);

  static {
    HttpURLConnection.setFollowRedirects(true);
  }

  public static final Text TOKEN_KIND = new Text("HFTP delegation");
  
  protected UserGroupInformation ugi; 
  private boolean remoteIsInsecure = false;
  private URI hftpURI;

  protected InetSocketAddress nnAddr;
  protected InetSocketAddress nnSecureAddr;  

  public static final String HFTP_TIMEZONE = "UTC";
  public static final String HFTP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

  private Token<?> delegationToken;
  private Token<?> renewToken;
  private static final HftpDelegationTokenSelector hftpTokenSelector =
      new HftpDelegationTokenSelector();

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
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  protected int getDefaultSecurePort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT);
  }

  protected InetSocketAddress getNamenodeAddr(URI uri) {
    // use authority so user supplied uri can override port
    return NetUtils.createSocketAddr(uri.getAuthority(), getDefaultPort());
  }

  protected InetSocketAddress getNamenodeSecureAddr(URI uri) {
    // must only use the host and the configured https port
    return NetUtils.makeSocketAddr(uri.getHost(), getDefaultSecurePort());
  }

  @Override
  public String getCanonicalServiceName() {
    // unlike other filesystems, hftp's service is the secure port, not the
    // actual port in the uri
    return SecurityUtil.buildTokenService(nnSecureAddr).toString();
  }

  @Override
  public void initialize(final URI name, final Configuration conf) 
  throws IOException {
    setConf(conf);
    super.initialize(name, conf);
    this.ugi = UserGroupInformation.getCurrentUser();
    this.nnAddr = getNamenodeAddr(name);
    this.nnSecureAddr = getNamenodeSecureAddr(name);
    this.hftpURI = DFSUtil.createUri(name.getScheme(), nnAddr);
  }
  
  protected void initDelegationToken() throws IOException {
    // look for hftp token, then try hdfs
    Token<?> token = selectHftpDelegationToken();
    if (token == null) {
      token = selectHdfsDelegationToken();
    }   

    //since we don't already have a token, go get one over https
    boolean createdToken = false;
    if (token == null) {
      token = getDelegationToken(null);
      createdToken = (token != null);
    }

    // security might be disabled
    if (token != null) {
      setDelegationToken(token);
      if (createdToken) {
        dtRenewer.addRenewAction(this);
        LOG.debug("Created new DT for " + token.getService());
      } else {
        LOG.debug("Found existing DT for " + token.getService());        
      }
    }
  }

  protected Token<DelegationTokenIdentifier> selectHftpDelegationToken() {
    Text serviceName = SecurityUtil.buildTokenService(nnSecureAddr);
    return hftpTokenSelector.selectToken(serviceName, ugi.getTokens());      
  }
  
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
    // NOTE: the remote nn must be configured to use hdfs
    delegationToken.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
    // no need to change service because we aren't exactly sure what it
    // should be.  we can guess, but it might be wrong if the local conf
    // value is incorrect.  the service is a client side field, so the remote
    // end does not care about the value
  }

  @Override
  public synchronized Token<?> getDelegationToken(final String renewer
                                                  ) throws IOException {
    try {
      //Renew TGT if needed
      ugi.checkTGTAndReloginFromKeytab();
      return ugi.doAs(new PrivilegedExceptionAction<Token<?>>() {
        public Token<?> run() throws IOException {
          final String nnHttpUrl = DFSUtil.createUri("https", nnSecureAddr).toString();
          Credentials c;
          try {
            c = DelegationTokenFetcher.getDTfromRemote(nnHttpUrl, renewer);
          } catch (Exception e) {
            LOG.info("Couldn't get a delegation token from " + nnHttpUrl + 
            " using https.");
            LOG.debug("error was ", e);
            //Maybe the server is in unsecure mode (that's bad but okay)
            remoteIsInsecure = true;
            return null;
          }
          for (Token<? extends TokenIdentifier> t : c.getAllTokens()) {
            LOG.debug("Got dt for " + getUri() + ";t.service="
                      +t.getService());
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
   * ugi parameter for http connection
   * 
   * @return user_shortname,group1,group2...
   */
  private String getUgiParameter() {
    StringBuilder ugiParamenter = new StringBuilder(ugi.getShortUserName());
    for(String g: ugi.getGroupNames()) {
      ugiParamenter.append(",");
      ugiParamenter.append(g);
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
    try {
      query = updateQuery(query);
      final URL url = new URI("http", null, nnAddr.getHostName(),
          nnAddr.getPort(), path, query, null).toURL();
      if (LOG.isTraceEnabled()) {
        LOG.trace("url=" + url);
      }
      return (HttpURLConnection)url.openConnection();
    } catch (URISyntaxException e) {
      throw (IOException)new IOException().initCause(e);
    }
  }
  
  protected String updateQuery(String query) throws IOException {
    String tokenString = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      synchronized (this) {
        if (delegationToken == null && !remoteIsInsecure) {
          initDelegationToken();
        }    
        if (delegationToken != null) {
          tokenString = delegationToken.encodeToUrlString();
          return (query + JspHelper.getDelegationTokenUrlParam(tokenString));
        } // else we are talking to an insecure cluster
      }
    }
    return query;
  }

  @Override
  public FSDataInputStream open(Path f, int buffersize) throws IOException {
    final HttpURLConnection connection = openConnection(
        "/data" + f.makeQualified(this).toUri().getPath(),
        "ugi=" + getUgiParameter());
    final InputStream in;
    try {
      connection.setRequestMethod("GET");
      connection.connect();
      in = connection.getInputStream();
    } catch(IOException ioe) {
      final int code = connection.getResponseCode();
      final String s = connection.getResponseMessage();
      throw s == null? ioe:
          new IOException(s + " (error code=" + code + ")", ioe);
    }

    final String cl = connection.getHeaderField(StreamFile.CONTENT_LENGTH);
    final long filelength = cl == null? -1: Long.parseLong(cl);
    if (LOG.isDebugEnabled()) {
      LOG.debug("filelength = " + filelength);
    }

    return new FSDataInputStream(new FSInputStream() {
        long currentPos = 0;

        private void update(final boolean isEOF, final int n
            ) throws IOException {
          if (!isEOF) {
            currentPos += n;
          } else if (currentPos < filelength) {
            throw new IOException("Got EOF but byteread = " + currentPos
                + " < filelength = " + filelength);
          }
        }
        public int read() throws IOException {
          final int b = in.read();
          update(b == -1, 1);
          return b;
        }
        public int read(byte[] b, int off, int len) throws IOException {
          final int n = in.read(b, off, len);
          update(n == -1, n);
          return n;
        }

        public void close() throws IOException {
          in.close();
        }

        public void seek(long pos) throws IOException {
          throw new IOException("Can't seek!");
        }
        public long getPos() throws IOException {
          throw new IOException("Position unknown!");
        }
        public boolean seekToNewSource(long targetPos) throws IOException {
          return false;
        }
      });
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
              new Path(getUri().toString(), attrs.getValue("path"))
                .makeQualified(HftpFileSystem.this))
        : new FileStatus(0L, true, 0, 0L,
              modif, atime, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              new Path(getUri().toString(), attrs.getValue("path"))
                .makeQualified(HftpFileSystem.this));
      fslist.add(fs);
    }
    
    private void fetchList(String path, boolean recur) throws IOException {
      try {
        XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        HttpURLConnection connection = openConnection("/listPaths" + path,
            "ugi=" + getUgiParameter() + (recur? "&recursive=yes" : ""));
        connection.setRequestMethod("GET");
        connection.connect();

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
      if (fslist.size() > 0 && (fslist.size() != 1 || fslist.get(0).isDir())) {
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
          "/fileChecksum" + f, "ugi=" + getUgiParameter());
      try {
        final XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);

        connection.setRequestMethod("GET");
        connection.connect();

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
    return new Path("/").makeQualified(this);
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
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  /*
   * @deprecated Use delete(path, boolean)
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
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
          "/contentSummary" + path, "ugi=" + getUgiParameter());
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
      InetSocketAddress serviceAddr = SecurityUtil.getTokenServiceAddr(token);
      return DelegationTokenFetcher.renewDelegationToken(
          DFSUtil.createUri("https", serviceAddr).toString(),
          (Token<DelegationTokenIdentifier>) token
      );
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cancel(Token<?> token, 
                       Configuration conf) throws IOException {
      // update the kerberos credentials, if they are coming from a keytab
      UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      // use https to cancel the token
      InetSocketAddress serviceAddr = SecurityUtil.getTokenServiceAddr(token);
      DelegationTokenFetcher.cancelDelegationToken(
          DFSUtil.createUri("https", serviceAddr).toString(), 
          (Token<DelegationTokenIdentifier>) token
      );
    }
    
  }
  
  private static class HftpDelegationTokenSelector
  extends AbstractDelegationTokenSelector<DelegationTokenIdentifier> {

    public HftpDelegationTokenSelector() {
      super(TOKEN_KIND);
    }
  }
}
