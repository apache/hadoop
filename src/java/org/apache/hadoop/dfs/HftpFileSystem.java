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

package org.apache.hadoop.dfs;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import javax.security.auth.login.LoginException;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/** An implementation of a protocol for accessing filesystems over HTTP.
 * The following implementation provides a limited, read-only interface
 * to a filesystem over HTTP.
 * @see org.apache.hadoop.dfs.ListPathsServlet
 * @see org.apache.hadoop.dfs.FileDataServlet
 */
public class HftpFileSystem extends FileSystem {
  static {
    HttpURLConnection.setFollowRedirects(true);
  }

  private String fshostname = "";
  private int fsport = -1;
  protected static final SimpleDateFormat df = ListPathsServlet.df;
  private UserGroupInformation ugi; 

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    setConf(conf);
    try {
      this.ugi = UnixUserGroupInformation.login(conf);
    } catch (LoginException le) {
      throw new IOException(StringUtils.stringifyException(le));
    } 

    this.fshostname = name.getHost();
    this.fsport = name.getPort();
    if(fsport >= 0)
      return;
    String infoAddr = 
      NetUtils.getServerAddress(conf, 
                                "dfs.info.bindAddress", 
                                "dfs.info.port", 
                                "dfs.http.address");
    this.fsport = NetUtils.createSocketAddr(infoAddr).getPort();
  }

  @Override
  public URI getUri() {
    try {
      return new URI("hftp", null, fshostname, fsport, null, null, null);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  @Override
  public FSDataInputStream open(Path f, int buffersize) throws IOException {
    HttpURLConnection connection = null;
    try {
      final URL url = new URI("http", null, fshostname, fsport,
          "/data" + f.toUri().getPath(), "ugi=" + ugi, null).toURL();
      connection = (HttpURLConnection)url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();
    } catch (URISyntaxException e) {
      IOException ie = new IOException("invalid url");
      ie.initCause(e);
      throw ie;
    }
    final InputStream in = connection.getInputStream();
    return new FSDataInputStream(new FSInputStream() {
        public int read() throws IOException {
          return in.read();
        }
        public int read(byte[] b, int off, int len) throws IOException {
          return in.read(b, off, len);
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
        throw new SAXException("Unrecognized entry: " + qname);
      }
      long modif;
      try {
        modif = df.parse(attrs.getValue("modified")).getTime();
      } catch (ParseException e) { throw new SAXException(e); }
      FileStatus fs = "file".equals(qname)
        ? new FileStatus(
              Long.valueOf(attrs.getValue("size")).longValue(), false,
              Short.valueOf(attrs.getValue("replication")).shortValue(),
              Long.valueOf(attrs.getValue("blocksize")).longValue(),
              modif, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              new Path("hftp", fshostname + ":" + fsport, 
                       attrs.getValue("path")))
        : new FileStatus(0L, true, 0, 0L,
              modif, FsPermission.valueOf(attrs.getValue("permission")),
              attrs.getValue("owner"), attrs.getValue("group"),
              new Path("hftp", fshostname + ":" + fsport, 
                       attrs.getValue("path")));
      fslist.add(fs);
    }

    private void fetchList(String path, boolean recur) throws IOException {
      try {
        XMLReader xr = XMLReaderFactory.createXMLReader();
        xr.setContentHandler(this);
        final URL url = new URI("http", null, fshostname, fsport,
            "/listPaths" + path, "ugi=" + ugi + (recur? "&recursive=yes" : ""),
            null).toURL();
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();

        InputStream resp = connection.getInputStream();
        xr.parse(new InputSource(resp));
      } catch (SAXException e) { 
        IOException ie = new IOException("invalid xml directory content");
        ie.initCause(e);
        throw ie;
      } catch (URISyntaxException e) {
        IOException ie = new IOException("invalid url");
        ie.initCause(e);
        throw ie;
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
  public boolean exists(Path f) throws IOException {
    LsParser lsparser = new LsParser();
    return lsparser.getFileStatus(f) != null;
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

  @Override
  public Path getWorkingDirectory() {
    return new Path("/").makeQualified(this);
  }

  @Override
  public void setWorkingDirectory(Path f) { }

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
  public boolean delete(Path f) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new IOException("Not supported");
  }

}
