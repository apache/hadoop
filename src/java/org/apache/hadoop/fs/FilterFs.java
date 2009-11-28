package org.apache.hadoop.fs;
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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A <code>FilterFs</code> contains some other file system, which it uses as its
 * basic file system, possibly transforming the data along the way or providing
 * additional functionality. The class <code>FilterFs</code> itself simply
 * overrides all methods of <code>AbstractFileSystem</code> with versions that
 * pass all requests to the contained file system. Subclasses of
 * <code>FilterFs</code> may further override some of these methods and may also
 * provide additional methods and fields.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class FilterFs extends AbstractFileSystem {
  private final AbstractFileSystem myFs;
  
  protected AbstractFileSystem getMyFs() {
    return myFs;
  }
  
  protected FilterFs(AbstractFileSystem fs) throws IOException,
      URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(),
        fs.getUri().getAuthority() != null, fs.getUriDefaultPort());
    myFs = fs;
  }

  @Override
  protected Path getInitialWorkingDirectory() {
    return myFs.getInitialWorkingDirectory();
  }
  
  @Override
  protected FSDataOutputStream createInternal(Path f,
    EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize,
    short replication, long blockSize, Progressable progress,
    int bytesPerChecksum, boolean createParent) throws IOException {
    checkPath(f);
    return myFs.createInternal(f, flag, absolutePermission, bufferSize,
        replication, blockSize, progress, bytesPerChecksum, createParent);
  }

  @Override
  protected boolean delete(Path f, boolean recursive) throws IOException {
    checkPath(f);
    return myFs.delete(f, recursive);
  }

  @Override
  protected BlockLocation[] getFileBlockLocations(Path f, long start, long len)
    throws IOException {
    checkPath(f);
    return myFs.getFileBlockLocations(f, start, len);
  }

  @Override
  protected FileChecksum getFileChecksum(Path f) throws IOException {
    checkPath(f);
    return myFs.getFileChecksum(f);
  }

  @Override
  protected FileStatus getFileStatus(Path f) throws IOException {
    checkPath(f);
    return myFs.getFileStatus(f);
  }

  @Override
  protected FsStatus getFsStatus() throws IOException {
    return myFs.getFsStatus();
  }

  @Override
  protected FsServerDefaults getServerDefaults() throws IOException {
    return myFs.getServerDefaults();
  }

  @Override
  protected int getUriDefaultPort() {
    return myFs.getUriDefaultPort();
  }

  @Override
  protected FileStatus[] listStatus(Path f) throws IOException {
    checkPath(f);
    return myFs.listStatus(f);
  }

  @Override
  protected void mkdir(Path dir, FsPermission permission, boolean createParent)
    throws IOException {
    checkPath(dir);
    myFs.mkdir(dir, permission, createParent);
    
  }

  @Override
  protected FSDataInputStream open(Path f, int bufferSize) throws IOException {
    checkPath(f);
    return myFs.open(f, bufferSize);
  }

  @Override
  protected void renameInternal(Path src, Path dst) throws IOException {
    checkPath(src);
    checkPath(dst);
    myFs.rename(src, dst, Options.Rename.NONE);
    
  }

  @Override
  protected void setOwner(Path f, String username, String groupname)
    throws IOException {
    checkPath(f);
    myFs.setOwner(f, username, groupname);
    
  }

  @Override
  protected void setPermission(Path f, FsPermission permission)
    throws IOException {
    checkPath(f);
    myFs.setPermission(f, permission);
  }

  @Override
  protected boolean setReplication(Path f, short replication)
    throws IOException {
    checkPath(f);
    return myFs.setReplication(f, replication);
  }

  @Override
  protected void setTimes(Path f, long mtime, long atime) throws IOException {
    checkPath(f);
    myFs.setTimes(f, mtime, atime);
    
  }

  @Override
  protected void setVerifyChecksum(boolean verifyChecksum) throws IOException {
    myFs.setVerifyChecksum(verifyChecksum);
  }
}
