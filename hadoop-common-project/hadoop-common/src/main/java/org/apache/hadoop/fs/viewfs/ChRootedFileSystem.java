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
package org.apache.hadoop.fs.viewfs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * <code>ChRootedFileSystem</code> is a file system with its root some path
 * below the root of its base file system. 
 * 
 * Example: For a base file system hdfs://nn1/ with chRoot at /usr/foo, the
 * members will be setup as shown below.
 * <ul>
 * <li>myFs is the base file system and points to hdfs at nn1</li>
 * <li>myURI is hdfs://nn1/user/foo</li>
 * <li>chRootPathPart is /user/foo</li>
 * <li>workingDir is a directory related to chRoot</li>
 * </ul>
 * 
 * The paths are resolved as follows by ChRootedFileSystem:
 * <ul>
 * <li> Absolute path /a/b/c is resolved to /user/foo/a/b/c at myFs</li>
 * <li> Relative path x/y is resolved to /user/foo/<workingDir>/x/y</li>
 * </ul>
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
class ChRootedFileSystem extends FileSystem {
  private final FileSystem myFs; // the base file system whose root is changed
  private final URI myUri; // the base URI + the chRoot
  private final Path chRootPathPart; // the root below the root of the base
  private final String chRootPathPartString;
  private Path workingDir;
  
  protected FileSystem getMyFs() {
    return myFs;
  }
  
  /**
   * @param path
   * @return  full path including the chroot 
   */
  protected Path fullPath(final Path path) {
    super.checkPath(path);
    return path.isAbsolute() ? 
        new Path(chRootPathPartString + path.toUri().getPath()) :
        new Path(chRootPathPartString + workingDir.toUri().getPath(), path);
  }
  
  /**
   * Constructor
   * @param fs base file system
   * @param theRoot chRoot for this file system
   * @throws URISyntaxException
   */
  public ChRootedFileSystem(final FileSystem fs, final Path theRoot)
    throws URISyntaxException {
    myFs = fs;
    myFs.makeQualified(theRoot); //check that root is a valid path for fs
                            // Would like to call myFs.checkPath(theRoot); 
                            // but not public
    chRootPathPart = new Path(theRoot.toUri().getPath());
    chRootPathPartString = chRootPathPart.toUri().getPath();
    try {
      initialize(fs.getUri(), fs.getConf());
    } catch (IOException e) { // This exception should not be thrown
      throw new RuntimeException("This should not occur");
    }
    
    /*
     * We are making URI include the chrootedPath: e.g. file:///chrootedPath.
     * This is questionable since Path#makeQualified(uri, path) ignores
     * the pathPart of a uri. Since this class is internal we can ignore
     * this issue but if we were to make it external then this needs
     * to be resolved.
     */
    // Handle the two cases:
    //              scheme:/// and scheme://authority/
    myUri = new URI(myFs.getUri().toString() + 
        (myFs.getUri().getAuthority() == null ? "" :  Path.SEPARATOR) +
          chRootPathPart.toString().substring(1));

    workingDir = getHomeDirectory();
    // We don't use the wd of the myFs
  }
  
  /** 
   * Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  public void initialize(final URI name, final Configuration conf)
      throws IOException {
    myFs.initialize(name, conf);
    super.initialize(name, conf);
    setConf(conf);
  }

  @Override
  public URI getUri() {
    return myUri;
  }
  
  @Override
  public Path makeQualified(final Path path) {
    return myFs.makeQualified(path);
    // NOT myFs.makeQualified(fullPath(path));
  }
 
  /**
   * Strip out the root from the path.
   * @param p - fully qualified path p
   * @return -  the remaining path  without the begining /
   * @throws IOException if the p is not prefixed with root
   */
  String stripOutRoot(final Path p) throws IOException {
    try {
     checkPath(p);
    } catch (IllegalArgumentException e) {
      throw new IOException("Internal Error - path " + p +
          " should have been with URI: " + myUri);
    }
    String pathPart = p.toUri().getPath();
    return (pathPart.length() == chRootPathPartString.length()) ? "" : pathPart
        .substring(chRootPathPartString.length() + 1);   
  }
  
  @Override
  protected Path getInitialWorkingDirectory() {
    /*
     * 3 choices here: 
     *     null or / or /user/<uname> or strip out the root out of myFs's
     *  inital wd. 
     * Only reasonable choice for initialWd for chrooted fds is null 
     * so that the default rule for wd is applied
     */
    return null;
  }
  
  public Path getResolvedQualifiedPath(final Path f)
      throws FileNotFoundException {
    return myFs.makeQualified(
        new Path(chRootPathPartString + f.toUri().toString()));
  }
  
  @Override
  public Path getHomeDirectory() {
    return  new Path("/user/"+System.getProperty("user.name")).makeQualified(
          getUri(), null);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }
  
  @Override
  public void setWorkingDirectory(final Path new_dir) {
    workingDir = new_dir.isAbsolute() ? new_dir : new Path(workingDir, new_dir);
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    return myFs.create(fullPath(f), permission, overwrite, bufferSize,
        replication, blockSize, progress);
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) 
      throws IOException {
    return myFs.delete(fullPath(f), recursive);
  }
  

  @Override
  @SuppressWarnings("deprecation")
  public boolean delete(Path f) throws IOException {
   return delete(f, true);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus fs, final long start,
      final long len) throws IOException {
    return myFs.getFileBlockLocations(
        new ViewFsFileStatus(fs, fullPath(fs.getPath())), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f) 
      throws IOException {
    return myFs.getFileChecksum(fullPath(f));
  }

  @Override
  public FileStatus getFileStatus(final Path f) 
      throws IOException {
    return myFs.getFileStatus(fullPath(f));
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return myFs.getStatus(fullPath(p));
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return myFs.getServerDefaults();
  }

  @Override
  public FileStatus[] listStatus(final Path f) 
      throws IOException {
    return myFs.listStatus(fullPath(f));
  }
  
  @Override
  public boolean mkdirs(final Path f, final FsPermission permission)
      throws IOException {
    return myFs.mkdirs(fullPath(f), permission);
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize) 
    throws IOException {
    return myFs.open(fullPath(f), bufferSize);
  }
  
  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    return myFs.append(fullPath(f), bufferSize, progress);
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    return myFs.rename(fullPath(src), fullPath(dst)); 
  }
  
  @Override
  public void setOwner(final Path f, final String username,
      final String groupname)
    throws IOException {
    myFs.setOwner(fullPath(f), username, groupname);
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
    throws IOException {
    myFs.setPermission(fullPath(f), permission);
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
    throws IOException {
    return myFs.setReplication(fullPath(f), replication);
  }

  @Override
  public void setTimes(final Path f, final long mtime, final long atime) 
      throws IOException {
    myFs.setTimes(fullPath(f), mtime, atime);
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum)  {
    myFs.setVerifyChecksum(verifyChecksum);
  }
  
  @Override
  public List<Token<?>> getDelegationTokens(String renewer) throws IOException {
    return myFs.getDelegationTokens(renewer);
  }
}
