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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * Implementation of AbstractFileSystem based on the existing implementation of 
 * {@link FileSystem}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class DelegateToFileSystem extends AbstractFileSystem {
  protected final FileSystem fsImpl;
  
  protected DelegateToFileSystem(URI theUri, FileSystem theFsImpl,
      Configuration conf, String supportedScheme, boolean authorityRequired)
      throws IOException, URISyntaxException {
    super(theUri, supportedScheme, authorityRequired, 
        FileSystem.getDefaultUri(conf).getPort());
    fsImpl = theFsImpl;
    fsImpl.initialize(theUri, conf);
    fsImpl.statistics = getStatistics();
  }

  @Override
  public Path getInitialWorkingDirectory() {
    return fsImpl.getInitialWorkingDirectory();
  }
  
  @Override
  @SuppressWarnings("deprecation") // call to primitiveCreate
  public FSDataOutputStream createInternal (Path f,
      EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize,
      short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt, boolean createParent) throws IOException {
    checkPath(f);
    
    // Default impl assumes that permissions do not matter
    // calling the regular create is good enough.
    // FSs that implement permissions should override this.

    if (!createParent) { // parent must exist.
      // since this.create makes parent dirs automatically
      // we must throw exception if parent does not exist.
      final FileStatus stat = getFileStatus(f.getParent());
      if (stat == null) {
        throw new FileNotFoundException("Missing parent:" + f);
      }
      if (!stat.isDirectory()) {
          throw new ParentNotDirectoryException("parent is not a dir:" + f);
      }
      // parent does exist - go ahead with create of file.
    }
    return fsImpl.primitiveCreate(f, absolutePermission, flag,
        bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    checkPath(f);
    return fsImpl.delete(f, recursive);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
      throws IOException {
    checkPath(f);
    return fsImpl.getFileBlockLocations(f, start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    checkPath(f);
    return fsImpl.getFileChecksum(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    checkPath(f);
    return fsImpl.getFileStatus(f);
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    return getFileStatus(f);
  }

  @Override
  public FsStatus getFsStatus() throws IOException {
    return fsImpl.getStatus();
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return fsImpl.getServerDefaults();
  }
  
  @Override
  public Path getHomeDirectory() {
    return fsImpl.getHomeDirectory();
  }

  @Override
  public int getUriDefaultPort() {
    return 0;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    checkPath(f);
    return fsImpl.listStatus(f);
  }

  @Override
  @SuppressWarnings("deprecation") // call to primitiveMkdir
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
      throws IOException {
    checkPath(dir);
    fsImpl.primitiveMkdir(dir, permission, createParent);
    
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    checkPath(f);
    return fsImpl.open(f, bufferSize);
  }

  @Override
  @SuppressWarnings("deprecation") // call to rename
  public void renameInternal(Path src, Path dst) throws IOException {
    checkPath(src);
    checkPath(dst);
    fsImpl.rename(src, dst, Options.Rename.NONE);
  }

  @Override
  public void setOwner(Path f, String username, String groupname)
      throws IOException {
    checkPath(f);
    fsImpl.setOwner(f, username, groupname);
  }

  @Override
  public void setPermission(Path f, FsPermission permission)
      throws IOException {
    checkPath(f);
    fsImpl.setPermission(f, permission);
  }

  @Override
  public boolean setReplication(Path f, short replication)
      throws IOException {
    checkPath(f);
    return fsImpl.setReplication(f, replication);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws IOException {
    checkPath(f);
    fsImpl.setTimes(f, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) throws IOException {
    fsImpl.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public boolean supportsSymlinks() {
    return false;
  }  
  
  @Override
  public void createSymlink(Path target, Path link, boolean createParent) 
      throws IOException { 
    throw new IOException("File system does not support symlinks");
  } 
  
  @Override
  public Path getLinkTarget(final Path f) throws IOException {
    /* We should never get here. Any file system that threw an 
     * UnresolvedLinkException, causing this function to be called,
     * should override getLinkTarget. 
     */
    throw new AssertionError();
  }

  @Override //AbstractFileSystem
  public String getCanonicalServiceName() {
    return fsImpl.getCanonicalServiceName();
  }
  
  @Override //AbstractFileSystem
  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
    return Arrays.asList(fsImpl.addDelegationTokens(renewer, null));
  }
}