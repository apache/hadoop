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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.impl.CorruptFileBlockIterator;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.Progressable;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Hdfs extends AbstractFileSystem {

  DFSClient dfs;
  private boolean verifyChecksum = true;

  static {
    HdfsConfiguration.init();
  }

  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}
   * 
   * @param theUri which must be that of Hdfs
   * @param conf configuration
   * @throws IOException
   */
  Hdfs(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
    super(theUri, HdfsConstants.HDFS_URI_SCHEME, true,
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);

    if (!theUri.getScheme().equalsIgnoreCase(HdfsConstants.HDFS_URI_SCHEME)) {
      throw new IllegalArgumentException("Passed URI's scheme is not for Hdfs");
    }
    String host = theUri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: " + theUri);
    }

    this.dfs = new DFSClient(theUri, conf, getStatistics());
  }

  @Override
  public int getUriDefaultPort() {
    return HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT;
  }

  @Override
  public HdfsDataOutputStream createInternal(Path f,
      EnumSet<CreateFlag> createFlag, FsPermission absolutePermission,
      int bufferSize, short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt, boolean createParent) throws IOException {

    final DFSOutputStream dfsos = dfs.primitiveCreate(getUriPath(f),
      absolutePermission, createFlag, createParent, replication, blockSize,
      progress, bufferSize, checksumOpt);
    return dfs.createWrappedOutputStream(dfsos, statistics,
        dfsos.getInitialLen());
  }

  @Override
  public boolean delete(Path f, boolean recursive) 
      throws IOException, UnresolvedLinkException {
    return dfs.delete(getUriPath(f), recursive);
  }

  /**
   * The returned BlockLocation will have different formats for replicated
   * and erasure coded file.
   *
   * Please refer to
   * {@link FileContext#getFileBlockLocations(Path, long, long)}
   * for more details.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException, UnresolvedLinkException {
    return dfs.getBlockLocations(getUriPath(p), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) 
      throws IOException, UnresolvedLinkException {
    return dfs.getFileChecksum(getUriPath(f), Long.MAX_VALUE);
  }

  @Override
  public FileStatus getFileStatus(Path f) 
      throws IOException, UnresolvedLinkException {
    HdfsFileStatus fi = dfs.getFileInfo(getUriPath(f));
    if (fi != null) {
      return fi.makeQualified(getUri(), f);
    } else {
      throw new FileNotFoundException("File does not exist: " + f.toString());
    }
  }
  
  @Override
  public FileStatus getFileLinkStatus(Path f) 
      throws IOException, UnresolvedLinkException {
    HdfsFileStatus fi = dfs.getFileLinkInfo(getUriPath(f));
    if (fi != null) {
      return fi.makeQualified(getUri(), f);
    } else {
      throw new FileNotFoundException("File does not exist: " + f);
    }
  }

  @Override
  public FsStatus getFsStatus() throws IOException {
    return dfs.getDiskStatus();
  }

  @Override
  @Deprecated
  public FsServerDefaults getServerDefaults() throws IOException {
    return dfs.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(final Path f) throws IOException {
    return dfs.getServerDefaults();
  }

  /**
   * The BlockLocation of returned LocatedFileStatus will have different
   * formats for replicated and erasure coded file.
   * Please refer to
   * {@link FileContext#getFileBlockLocations(Path, long, long)} for
   * more details.
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(
      final Path p)
      throws FileNotFoundException, IOException {
    return new DirListingIterator<LocatedFileStatus>(p, true) {

      @Override
      public LocatedFileStatus next() throws IOException {
        return ((HdfsLocatedFileStatus)getNext()).makeQualifiedLocated(
            getUri(), p);
      }
    };
  }
  
  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    return new DirListingIterator<FileStatus>(f, false) {

      @Override
      public FileStatus next() throws IOException {
        return getNext().makeQualified(getUri(), f);
      }
    };
  }

  /**
   * This class defines an iterator that returns
   * the file status of each file/subdirectory of a directory
   * 
   * if needLocation, status contains block location if it is a file
   * throws a RuntimeException with the error as its cause.
   * 
   * @param <T> the type of the file status
   */
  abstract private class  DirListingIterator<T extends FileStatus>
  implements RemoteIterator<T> {
    private DirectoryListing thisListing;
    private int i;
    final private String src;
    final private boolean needLocation;  // if status

    private DirListingIterator(Path p, boolean needLocation)
      throws IOException {
      this.src = Hdfs.this.getUriPath(p);
      this.needLocation = needLocation;

      // fetch the first batch of entries in the directory
      thisListing = dfs.listPaths(
          src, HdfsFileStatus.EMPTY_NAME, needLocation);
      if (thisListing == null) { // the directory does not exist
        throw new FileNotFoundException("File " + src + " does not exist.");
      }
    }

    @Override
    public boolean hasNext() throws IOException {
      if (thisListing == null) {
        return false;
      }
      if (i>=thisListing.getPartialListing().length
          && thisListing.hasMore()) { 
        // current listing is exhausted & fetch a new listing
        thisListing = dfs.listPaths(src, thisListing.getLastName(),
            needLocation);
        if (thisListing == null) {
          throw new FileNotFoundException("File " + src + " does not exist.");
        }
        i = 0;
      }
      return (i<thisListing.getPartialListing().length);
    }

    /**
     * Get the next item in the list
     * @return the next item in the list
     * 
     * @throws IOException if there is any error
     * @throws NoSuchElementException if no more entry is available
     */
    public HdfsFileStatus getNext() throws IOException {
      if (hasNext()) {
        return thisListing.getPartialListing()[i++];
      }
      throw new NoSuchElementException("No more entry in " + src);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) 
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
        stats[i] = partialListing[i].makeQualified(getUri(), f);
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
      listing.add(fileStatus.makeQualified(getUri(), f));
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
        listing.add(fileStatus.makeQualified(getUri(), f));
      }
    } while (thisListing.hasMore());
 
    return listing.toArray(new FileStatus[listing.size()]);
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
    throws IOException {
    return new CorruptFileBlockIterator(dfs, path);
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
    throws IOException, UnresolvedLinkException {
    dfs.primitiveMkdir(getUriPath(dir), permission, createParent);
  }

  @SuppressWarnings("deprecation")
  @Override
  public HdfsDataInputStream open(Path f, int bufferSize) 
      throws IOException, UnresolvedLinkException {
    final DFSInputStream dfsis = dfs.open(getUriPath(f),
      bufferSize, verifyChecksum);
    return dfs.createWrappedInputStream(dfsis);
  }

  @Override
  public boolean truncate(Path f, long newLength)
      throws IOException, UnresolvedLinkException {
    return dfs.truncate(getUriPath(f), newLength);
  }

  @Override
  public void renameInternal(Path src, Path dst) 
    throws IOException, UnresolvedLinkException {
    dfs.rename(getUriPath(src), getUriPath(dst), Options.Rename.NONE);
  }

  @Override
  public void renameInternal(Path src, Path dst, boolean overwrite)
      throws IOException, UnresolvedLinkException {
    dfs.rename(getUriPath(src), getUriPath(dst),
        overwrite ? Options.Rename.OVERWRITE : Options.Rename.NONE);
  }

  @Override
  public void setOwner(Path f, String username, String groupname)
    throws IOException, UnresolvedLinkException {
    dfs.setOwner(getUriPath(f), username, groupname);
  }

  @Override
  public void setPermission(Path f, FsPermission permission)
    throws IOException, UnresolvedLinkException {
    dfs.setPermission(getUriPath(f), permission);
  }

  @Override
  public boolean setReplication(Path f, short replication)
    throws IOException, UnresolvedLinkException {
    return dfs.setReplication(getUriPath(f), replication);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) 
    throws IOException, UnresolvedLinkException {
    dfs.setTimes(getUriPath(f), mtime, atime);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) 
    throws IOException {
    this.verifyChecksum = verifyChecksum;
  }
  
  @Override
  public boolean supportsSymlinks() {
    return true;
  }  
  
  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
    throws IOException, UnresolvedLinkException {
    dfs.createSymlink(target.toString(), getUriPath(link), createParent);
  }

  @Override
  public Path getLinkTarget(Path p) throws IOException { 
    return new Path(dfs.getLinkTarget(getUriPath(p)));
  }
  
  @Override
  public String getCanonicalServiceName() {
    return dfs.getCanonicalServiceName();
  }

  @Override //AbstractFileSystem
  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
    Token<DelegationTokenIdentifier> result = dfs
        .getDelegationToken(renewer == null ? null : new Text(renewer));
    List<Token<?>> tokenList = new ArrayList<Token<?>>();
    tokenList.add(result);
    return tokenList;
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    dfs.modifyAclEntries(getUriPath(path), aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    dfs.removeAclEntries(getUriPath(path), aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    dfs.removeDefaultAcl(getUriPath(path));
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    dfs.removeAcl(getUriPath(path));
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    dfs.setAcl(getUriPath(path), aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return dfs.getAclStatus(getUriPath(path));
  }
  
  @Override
  public void setXAttr(Path path, String name, byte[] value, 
      EnumSet<XAttrSetFlag> flag) throws IOException {
    dfs.setXAttr(getUriPath(path), name, value, flag);
  }
  
  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    return dfs.getXAttr(getUriPath(path), name);
  }
  
  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    return dfs.getXAttrs(getUriPath(path));
  }
  
  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) 
      throws IOException {
    return dfs.getXAttrs(getUriPath(path), names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    return dfs.listXAttrs(getUriPath(path));
  }
  
  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    dfs.removeXAttr(getUriPath(path), name);
  }

  @Override
  public void access(Path path, final FsAction mode) throws IOException {
    dfs.checkAccess(getUriPath(path), mode);
  }

  @Override
  public void setStoragePolicy(Path path, String policyName) throws IOException {
    dfs.setStoragePolicy(getUriPath(path), policyName);
  }

  @Override
  public void unsetStoragePolicy(final Path src) throws IOException {
    dfs.unsetStoragePolicy(getUriPath(src));
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    return dfs.getStoragePolicy(getUriPath(src));
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return Arrays.asList(dfs.getStoragePolicies());
  }

  /**
   * Renew an existing delegation token.
   * 
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use Token.renew instead.
   */
  @SuppressWarnings("unchecked")
  public long renewDelegationToken(
      Token<? extends AbstractDelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return dfs.renewDelegationToken((Token<DelegationTokenIdentifier>) token);
  }

  /**
   * Cancel an existing delegation token.
   * 
   * @param token delegation token
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use Token.cancel instead.
   */
  @SuppressWarnings("unchecked")
  public void cancelDelegationToken(
      Token<? extends AbstractDelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    dfs.cancelDelegationToken((Token<DelegationTokenIdentifier>) token);
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    return new Path(dfs.createSnapshot(getUriPath(path), snapshotName));
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    dfs.renameSnapshot(getUriPath(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(final Path snapshotDir, final String snapshotName)
      throws IOException {
    dfs.deleteSnapshot(getUriPath(snapshotDir), snapshotName);
  }
}
