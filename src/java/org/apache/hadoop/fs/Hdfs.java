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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

@InterfaceAudience.Private
@InterfaceStability.Evolving
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
  protected boolean delete(Path f, boolean recursive) 
      throws IOException, UnresolvedLinkException {
    return dfs.delete(getUriPath(f), recursive);
  }

  @Override
  protected BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException, UnresolvedLinkException {
    return dfs.getBlockLocations(getUriPath(p), start, len);
  }

  @Override
  protected FileChecksum getFileChecksum(Path f) 
      throws IOException, UnresolvedLinkException {
    return dfs.getFileChecksum(getUriPath(f));
  }

  @Override
  protected FileStatus getFileStatus(Path f) 
      throws IOException, UnresolvedLinkException {
    HdfsFileStatus fi = dfs.getFileInfo(getUriPath(f));
    if (fi != null) {
      return makeQualified(fi, f);
    } else {
      throw new FileNotFoundException("File does not exist: " + f.toString());
    }
  }
  
  @Override
  public FileStatus getFileLinkStatus(Path f) 
      throws IOException, UnresolvedLinkException {
    HdfsFileStatus fi = dfs.getFileLinkInfo(getUriPath(f));
    if (fi != null) {
      return makeQualified(fi, f);
    } else {
      throw new FileNotFoundException("File does not exist: " + f);
    }
  }  

  private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
    // NB: symlink is made fully-qualified in FileContext. 
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(),
        f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.isSymlink() ? new Path(f.getSymlink()) : null,
        (f.getFullPath(parent)).makeQualified(
            getUri(), null)); // fully-qualify path
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
  protected Iterator<FileStatus> listStatusIterator(final Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    return new Iterator<FileStatus>() {
      private DirectoryListing thisListing;
      private int i;
      private String src;


      { // initializer
        src = getUriPath(f);
        // fetch the first batch of entries in the directory
        thisListing = dfs.listPaths(src, HdfsFileStatus.EMPTY_NAME);
        if (thisListing == null) { // the directory does not exist
          throw new FileNotFoundException("File " + f + " does not exist.");
        }
      }

      @Override
      public boolean hasNext() {
        if (thisListing == null) {
          return false;
        }
        try {
          if (i>=thisListing.getPartialListing().length && thisListing.hasMore()) { 
            // current listing is exhausted & fetch a new listing
            thisListing = dfs.listPaths(src, thisListing.getLastName());
            if (thisListing == null) {
              return false; // the directory is deleted
            }
            i = 0;
          }
          return (i<thisListing.getPartialListing().length);
        } catch (IOException ioe) {
          return false;
        }
      }

      @Override
      public FileStatus next() {
        if (hasNext()) {
          return makeQualified(thisListing.getPartialListing()[i++], f);
        } 
        throw new java.util.NoSuchElementException("No more entry in " + f);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");

      }
    };
  }
  
  @Override
  protected FileStatus[] listStatus(Path f) 
      throws IOException, UnresolvedLinkException {
    String src = getUriPath(f);

    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = dfs.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);

    if (thisListing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + f + " does not exist.");
    }
    
    HdfsFileStatus[] partialListing = thisListing.getPartialListing();
    if (!thisListing.hasMore()) { // got all entries of the directory
      FileStatus[] stats = new FileStatus[partialListing.length];
      for (int i = 0; i < partialListing.length; i++) {
        stats[i] = makeQualified(partialListing[i], f);
      }
      return stats;
    }

    // The directory size is too big that it needs to fetch more
    // estimate the total number of entries in the directory
    int totalNumEntries =
      partialListing.length + thisListing.getRemainingEntries();
    ArrayList<FileStatus> listing =
      new ArrayList<FileStatus>(totalNumEntries);
    // add the first batch of entries to the array list
    for (HdfsFileStatus fileStatus : partialListing) {
      listing.add(makeQualified(fileStatus, f));
    }
 
    // now fetch more entries
    do {
      thisListing = dfs.listPaths(src, thisListing.getLastName());
 
      if (thisListing == null) {
        // the directory is deleted
        throw new FileNotFoundException("File " + f + " does not exist.");
      }
 
      partialListing = thisListing.getPartialListing();
      for (HdfsFileStatus fileStatus : partialListing) {
        listing.add(makeQualified(fileStatus, f));
      }
    } while (thisListing.hasMore());
 
    return listing.toArray(new FileStatus[listing.size()]);
  }

  @Override
  protected void mkdir(Path dir, FsPermission permission, boolean createParent)
    throws IOException, UnresolvedLinkException {
    dfs.mkdirs(getUriPath(dir), permission, createParent);
  }

  @Override
  protected FSDataInputStream open(Path f, int bufferSize) 
      throws IOException, UnresolvedLinkException {
    return new DFSClient.DFSDataInputStream(dfs.open(getUriPath(f),
        bufferSize, verifyChecksum));
  }

  @Override
  protected void renameInternal(Path src, Path dst) 
    throws IOException, UnresolvedLinkException {
    dfs.rename(getUriPath(src), getUriPath(dst));
  }

  @Override
  protected void renameInternal(Path src, Path dst, boolean overwrite)
      throws IOException, UnresolvedLinkException {
    dfs.rename(getUriPath(src), getUriPath(dst),
        overwrite ? Options.Rename.OVERWRITE : Options.Rename.NONE);
  }

  @Override
  protected void setOwner(Path f, String username, String groupname)
    throws IOException, UnresolvedLinkException {
    dfs.setOwner(getUriPath(f), username, groupname);
  }

  @Override
  protected void setPermission(Path f, FsPermission permission)
    throws IOException, UnresolvedLinkException {
    dfs.setPermission(getUriPath(f), permission);
  }

  @Override
  protected boolean setReplication(Path f, short replication)
    throws IOException, UnresolvedLinkException {
    return dfs.setReplication(getUriPath(f), replication);
  }

  @Override
  protected void setTimes(Path f, long mtime, long atime) 
    throws IOException, UnresolvedLinkException {
    dfs.setTimes(getUriPath(f), mtime, atime);
  }

  @Override
  protected void setVerifyChecksum(boolean verifyChecksum) 
    throws IOException {
    this.verifyChecksum = verifyChecksum;
  }
  
  @Override
  protected boolean supportsSymlinks() {
    return true;
  }  
  
  @Override
  protected void createSymlink(Path target, Path link, boolean createParent)
    throws IOException, UnresolvedLinkException {
    dfs.createSymlink(target.toString(), getUriPath(link), createParent);
  }

  @Override
  protected Path getLinkTarget(Path p) throws IOException { 
    return new Path(dfs.getLinkTarget(getUriPath(p)));
  }
}
