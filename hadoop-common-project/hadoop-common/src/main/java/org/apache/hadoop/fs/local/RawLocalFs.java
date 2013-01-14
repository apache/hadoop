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
package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

/**
 * The RawLocalFs implementation of AbstractFileSystem.
 *  This impl delegates to the old FileSystem
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class RawLocalFs extends DelegateToFileSystem {

  RawLocalFs(final Configuration conf) throws IOException, URISyntaxException {
    this(FsConstants.LOCAL_FS_URI, conf);
  }
  
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   * 
   * @param theUri which must be that of localFs
   * @param conf
   * @throws IOException
   * @throws URISyntaxException 
   */
  RawLocalFs(final URI theUri, final Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, new RawLocalFileSystem(), conf, 
        FsConstants.LOCAL_FS_URI.getScheme(), false);
  }
  
  @Override
  public int getUriDefaultPort() {
    return -1; // No default port for file:///
  }
  
  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return LocalConfigKeys.getServerDefaults();
  }
  
  @Override
  public boolean supportsSymlinks() {
    return true;
  }  
  
  @Override
  public void createSymlink(Path target, Path link, boolean createParent) 
      throws IOException {
    final String targetScheme = target.toUri().getScheme();
    if (targetScheme != null && !"file".equals(targetScheme)) {
      throw new IOException("Unable to create symlink to non-local file "+
                            "system: "+target.toString());
    }
    if (createParent) {
      mkdir(link.getParent(), FsPermission.getDirDefault(), true);
    }
    // NB: Use createSymbolicLink in java.nio.file.Path once available
    try {
      Shell.execCommand(Shell.LINK_COMMAND, "-s",
                        new URI(target.toString()).getPath(),
                        new URI(link.toString()).getPath());
    } catch (URISyntaxException x) {
      throw new IOException("Invalid symlink path: "+x.getMessage());
    } catch (IOException x) {
      throw new IOException("Unable to create symlink: "+x.getMessage());
    }
  }

  /** 
   * Returns the target of the given symlink. Returns the empty string if  
   * the given path does not refer to a symlink or there is an error 
   * acessing the symlink.
   */
  private String readLink(Path p) {
    /* NB: Use readSymbolicLink in java.nio.file.Path once available. Could
     * use getCanonicalPath in File to get the target of the symlink but that 
     * does not indicate if the given path refers to a symlink.
     */
    try {
      final String path = p.toUri().getPath();
      return Shell.execCommand(Shell.READ_LINK_COMMAND, path).trim(); 
    } catch (IOException x) {
      return "";
    }
  }
  
  /**
   * Return a FileStatus representing the given path. If the path refers 
   * to a symlink return a FileStatus representing the link rather than
   * the object the link refers to.
   */
  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    String target = readLink(f);
    try {
      FileStatus fs = getFileStatus(f);
      // If f refers to a regular file or directory      
      if (target.isEmpty()) {
        return fs;
      }
      // Otherwise f refers to a symlink
      return new FileStatus(fs.getLen(), 
          false,
          fs.getReplication(), 
          fs.getBlockSize(),
          fs.getModificationTime(),
          fs.getAccessTime(),
          fs.getPermission(),
          fs.getOwner(),
          fs.getGroup(),
          new Path(target),
          f);
    } catch (FileNotFoundException e) {
      /* The exists method in the File class returns false for dangling 
       * links so we can get a FileNotFoundException for links that exist.
       * It's also possible that we raced with a delete of the link. Use
       * the readBasicFileAttributes method in java.nio.file.attributes 
       * when available.
       */
      if (!target.isEmpty()) {
        return new FileStatus(0, false, 0, 0, 0, 0, FsPermission.getDefault(), 
            "", "", new Path(target), f);        
      }
      // f refers to a file or directory that does not exist
      throw e;
    }
  }
  
   @Override
   public boolean isValidName(String src) {
     // Different local file systems have different validation rules.  Skip
     // validation here and just let the OS handle it.  This is consistent with
     // RawLocalFileSystem.
     return true;
   }
  
  @Override
  public Path getLinkTarget(Path f) throws IOException {
    /* We should never get here. Valid local links are resolved transparently
     * by the underlying local file system and accessing a dangling link will 
     * result in an IOException, not an UnresolvedLinkException, so FileContext
     * should never call this function.
     */
    throw new AssertionError();
  }
}
