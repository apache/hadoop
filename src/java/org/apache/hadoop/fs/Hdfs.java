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
package org.apache.hadoop.fs;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.Progressable;

public class Hdfs extends AbstractFileSystem {

  DFSClient dfs;
  private boolean verifyChecksum = true;

  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}
   * 
   * @param theUri
   *          which must be that of Hdfs
   * @param conf
   * @throws IOException
   */
  Hdfs(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
    super(theUri, FSConstants.HDFS_URI_SCHEME, true, NameNode.DEFAULT_PORT);

    if (!theUri.getScheme().equalsIgnoreCase(FSConstants.HDFS_URI_SCHEME)) {
      throw new IllegalArgumentException("Passed URI's scheme is not for Hdfs");
    }
    String host = theUri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: " + theUri);
    }

    InetSocketAddress namenode = NameNode.getAddress(theUri.getAuthority());
    this.dfs = new DFSClient(namenode, conf, getStatistics());
  }

  @Override
  protected int getUriDefaultPort() {
    return NameNode.DEFAULT_PORT;
  }

  @Override
  protected FSDataOutputStream createInternal(Path f,
      EnumSet<CreateFlag> createFlag, FsPermission absolutePermission,
      int bufferSize, short replication, long blockSize, Progressable progress,
      int bytesPerChecksum, boolean createParent) throws IOException {
    return new FSDataOutputStream(dfs.primitiveCreate(getUriPath(f),
        absolutePermission, createFlag, createParent, replication, blockSize,
        progress, bufferSize, bytesPerChecksum), getStatistics());
  }

  @Override
  protected boolean delete(Path f, boolean recursive) throws IOException {
    return dfs.delete(getUriPath(f), recursive);
  }

  @Override
  protected BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    return dfs.getBlockLocations(p.toString(), start, len);
  }

  @Override
  protected FileChecksum getFileChecksum(Path f) throws IOException {
    return dfs.getFileChecksum(getUriPath(f));
  }

  @Override
  protected FileStatus getFileStatus(Path f) throws IOException {
    FileStatus fi = dfs.getFileInfo(getUriPath(f));
    if (fi != null) {
      fi.setPath(fi.getPath().makeQualified(getUri(), null));
      return fi;
    } else {
      throw new FileNotFoundException("File does not exist: " + f.toString());
    }
  }

  @Override
  protected FsStatus getFsStatus() throws IOException {
    return dfs.getDiskStatus();
  }

  @Override
  protected FsServerDefaults getServerDefaults() throws IOException {
    return dfs.getServerDefaults();
  }

  @Override
  protected FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] infos = dfs.listPaths(getUriPath(f));
    if (infos == null)
      throw new FileNotFoundException("File " + f + " does not exist.");

    for (int i = 0; i < infos.length; i++) {
      infos[i].setPath(infos[i].getPath().makeQualified(getUri(), null));
    }
    return infos;
  }

  @Override
  protected void mkdir(Path dir, FsPermission permission, boolean createParent)
    throws IOException {
    dfs.mkdirs(getUriPath(dir), permission, createParent);

  }

  @Override
  protected FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new DFSClient.DFSDataInputStream(dfs.open(getUriPath(f),
        bufferSize, verifyChecksum));
  }

  @Override
  protected void renameInternal(Path src, Path dst) throws IOException {
    dfs.rename(getUriPath(src), getUriPath(dst));
  }

  @Override
  protected void renameInternal(Path src, Path dst, boolean overwrite)
      throws IOException {
    dfs.rename(getUriPath(src), getUriPath(dst),
        overwrite ? Options.Rename.OVERWRITE : Options.Rename.NONE);
  }

  @Override
  protected void setOwner(Path f, String username, String groupname)
    throws IOException {
    dfs.setOwner(getUriPath(f), username, groupname);

  }

  @Override
  protected void setPermission(Path f, FsPermission permission)
    throws IOException {
    dfs.setPermission(getUriPath(f), permission);

  }

  @Override
  protected boolean setReplication(Path f, short replication)
    throws IOException {
    return dfs.setReplication(getUriPath(f), replication);
  }

  @Override
  protected void setTimes(Path f, long mtime, long atime) throws IOException {
    dfs.setTimes(getUriPath(f), mtime, atime);

  }

  @Override
  protected void setVerifyChecksum(boolean verifyChecksum) throws IOException {
    this.verifyChecksum = verifyChecksum;
  }
}
