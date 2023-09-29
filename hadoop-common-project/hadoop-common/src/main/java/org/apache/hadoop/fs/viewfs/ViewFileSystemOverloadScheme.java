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

import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_MOUNTTABLE_LOADER_IMPL;
import static org.apache.hadoop.fs.viewfs.Constants.DEFAULT_MOUNT_TABLE_CONFIG_LOADER_IMPL;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <p> This class is extended from the ViewFileSystem for the overloaded
 * scheme file system. Mount link configurations and in-memory mount table
 * building behaviors are inherited from ViewFileSystem. Unlike
 * ViewFileSystem scheme (viewfs://), the users would be able to use
 * any scheme. </p>
 *
 * <p> To use this class, the following configurations need to be added in
 * core-site.xml file. <br>
 * 1) fs.{@literal <scheme>}.impl
 *    = org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme <br>
 * 2) fs.viewfs.overload.scheme.target.{@literal <scheme>}.impl
 *    = {@literal <hadoop compatible file system implementation class name
 *    for the <scheme>>} </p>
 *
 * <p> Here {@literal <scheme>} can be any scheme, but with that scheme there
 * should be a hadoop compatible file system available. Second configuration
 * value should be the respective scheme's file system implementation class.
 * Example: if scheme is configured with "hdfs", then the 2nd configuration
 * class name will be org.apache.hadoop.hdfs.DistributedFileSystem.
 * if scheme is configured with "s3a", then the 2nd configuration class name
 * will be org.apache.hadoop.fs.s3a.S3AFileSystem. </p>
 *
 * <p> Use Case 1: <br>
 * =========== <br>
 * If users want some of their existing cluster (hdfs://Cluster)
 * data to mount with other hdfs and object store clusters(hdfs://NN1,
 * o3fs://bucket1.volume1/, s3a://bucket1/) </p>
 *
 * <p>
 * fs.viewfs.mounttable.Cluster.link./user = hdfs://NN1/user <br>
 * fs.viewfs.mounttable.Cluster.link./data = o3fs://bucket1.volume1/data <br>
 * fs.viewfs.mounttable.Cluster.link./backup = s3a://bucket1/backup/
 * </p>
 *
 * <p>
 * Op1: Create file hdfs://Cluster/user/fileA will go to hdfs://NN1/user/fileA
 * <br>
 * Op2: Create file hdfs://Cluster/data/datafile will go to
 *      o3fs://bucket1.volume1/data/datafile<br>
 * Op3: Create file hdfs://Cluster/backup/data.zip will go to
 *      s3a://bucket1/backup/data.zip
 * </p>
 *
 * <p> Use Case 2:<br>
 * ===========<br>
 * If users want some of their existing cluster (s3a://bucketA/)
 * data to mount with other hdfs and object store clusters
 * (hdfs://NN1, o3fs://bucket1.volume1/) </p>
 *
 * <p>
 * fs.viewfs.mounttable.bucketA.link./user = hdfs://NN1/user<br>
 * fs.viewfs.mounttable.bucketA.link./data = o3fs://bucket1.volume1/data<br>
 * fs.viewfs.mounttable.bucketA.link./salesDB = s3a://bucketA/salesDB/
 * </p>
 *
 * <p>
 * Op1: Create file s3a://bucketA/user/fileA will go to hdfs://NN1/user/fileA
 * <br>
 * Op2: Create file s3a://bucketA/data/datafile will go to
 *      o3fs://bucket1.volume1/data/datafile<br>
 * Op3: Create file s3a://bucketA/salesDB/dbfile will go to
 *      s3a://bucketA/salesDB/dbfile
 * </p>
 *
 * <p> Note:<br>
 * (1) In ViewFileSystemOverloadScheme, by default the mount links will be
 * represented as non-symlinks. If you want to change this behavior, please see
 * {@link ViewFileSystem#listStatus(Path)}<br>
 * (2) In ViewFileSystemOverloadScheme, only the initialized uri's hostname will
 * be considered as the mount table name. When the passed uri has hostname:port,
 * it will simply ignore the port number and only hostname will be considered as
 * the mount table name.<br>
 * (3) If there are no mount links configured with the initializing uri's
 * hostname as the mount table name, then it will automatically consider the
 * current uri as fallback( ex:
 * {@literal fs.viewfs.mounttable.<mycluster>.linkFallback}) target fs uri.
 * </p>
 */
@InterfaceAudience.LimitedPrivate({ "MapReduce", "HBase", "Hive" })
@InterfaceStability.Evolving
public class ViewFileSystemOverloadScheme extends ViewFileSystem {
  private URI myUri;
  private boolean supportAutoAddingFallbackOnNoMounts = true;
  public ViewFileSystemOverloadScheme() throws IOException {
    super();
  }

  @Override
  public String getScheme() {
    return myUri.getScheme();
  }

  /**
   * By default returns false as ViewFileSystemOverloadScheme supports auto
   * adding fallback on no mounts.
   */
  public boolean supportAutoAddingFallbackOnNoMounts() {
    return this.supportAutoAddingFallbackOnNoMounts;
  }

  /**
   * Sets whether to add fallback automatically when no mount points found.
   *
   * @param addAutoFallbackOnNoMounts addAutoFallbackOnNoMounts.
   */
  public void setSupportAutoAddingFallbackOnNoMounts(
      boolean addAutoFallbackOnNoMounts) {
    this.supportAutoAddingFallbackOnNoMounts = addAutoFallbackOnNoMounts;
  }

  @Override
  public void initialize(URI theUri, Configuration conf) throws IOException {
    this.myUri = theUri;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing the ViewFileSystemOverloadScheme with the uri: "
          + theUri);
    }
    String mountTableConfigPath =
        conf.get(Constants.CONFIG_VIEWFS_MOUNTTABLE_PATH);
    /* The default value to false in ViewFSOverloadScheme */
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS,
        conf.getBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS,
            false));
    /* the default value to true in ViewFSOverloadScheme */
    conf.setBoolean(CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME,
        conf.getBoolean(Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME,
            true));
    if (null != mountTableConfigPath) {
      MountTableConfigLoader loader = getMountTableConfigLoader(conf);
      loader.load(mountTableConfigPath, conf);
    } else {
      // TODO: Should we fail here.?
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Missing configuration for fs.viewfs.mounttable.path. Proceeding"
                + "with core-site.xml mount-table information if avaialable.");
      }
    }
    super.initialize(theUri, conf);
  }

  private MountTableConfigLoader getMountTableConfigLoader(
      final Configuration conf) {
    Class<? extends MountTableConfigLoader> clazz =
        conf.getClass(CONFIG_VIEWFS_MOUNTTABLE_LOADER_IMPL,
            DEFAULT_MOUNT_TABLE_CONFIG_LOADER_IMPL,
                MountTableConfigLoader.class);

    if (clazz == null) {
      throw new RuntimeException(
          String.format("Errors on getting mount table loader class. "
              + "The fs.viewfs.mounttable.config.loader.impl conf is %s. ",
                  conf.get(CONFIG_VIEWFS_MOUNTTABLE_LOADER_IMPL,
                      DEFAULT_MOUNT_TABLE_CONFIG_LOADER_IMPL.getName())));
    }

    try {
      MountTableConfigLoader mountTableConfigLoader =
          ReflectionUtils.newInstance(clazz, conf);
      return mountTableConfigLoader;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method is overridden because in ViewFileSystemOverloadScheme if
   * overloaded scheme matches with mounted target fs scheme, file system
   * should be created without going into {@literal fs.<scheme>.impl} based
   * resolution. Otherwise it will end up in an infinite loop as the target
   * will be resolved again to ViewFileSystemOverloadScheme as
   * {@literal fs.<scheme>.impl} points to ViewFileSystemOverloadScheme.
   * So, below method will initialize the
   * {@literal fs.viewfs.overload.scheme.target.<scheme>.impl}.
   * Other schemes can follow fs.newInstance
   */
  @Override
  protected FsGetter fsGetter() {
    return new ChildFsGetter(getScheme());
  }

  /**
   * This class checks whether the rooScheme is same as URI scheme. If both are
   * same, then it will initialize file systems by using the configured
   * {@literal fs.viewfs.overload.scheme.target.<scheme>.impl} class.
   */
  static class ChildFsGetter extends FsGetter {

    private final String rootScheme;

    ChildFsGetter(String rootScheme) {
      this.rootScheme = rootScheme;
    }

    @Override
    public FileSystem getNewInstance(URI uri, Configuration conf)
        throws IOException {
      if (uri.getScheme().equals(this.rootScheme)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "The file system initialized uri scheme is matching with the "
                  + "given target uri scheme. The target uri is: " + uri);
        }
        /*
         * Avoid looping when target fs scheme is matching to overloaded scheme.
         */
        return createFileSystem(uri, conf);
      } else {
        return FileSystem.newInstance(uri, conf);
      }
    }

    /**
     * When ViewFileSystemOverloadScheme scheme and target uri scheme are
     * matching, it will not take advantage of FileSystem cache as it will
     * create instance directly. For caching needs please set
     * "fs.viewfs.enable.inner.cache" to true.
     */
    @Override
    public FileSystem get(URI uri, Configuration conf) throws IOException {
      if (uri.getScheme().equals(this.rootScheme)) {
        // Avoid looping when target fs scheme is matching to overloaded
        // scheme.
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "The file system initialized uri scheme is matching with the "
                  + "given target uri scheme. So, the target file system "
                  + "instances will not be cached. To cache fs instances, "
                  + "please set fs.viewfs.enable.inner.cache to true. "
                  + "The target uri is: " + uri);
        }
        return createFileSystem(uri, conf);
      } else {
        return FileSystem.get(uri, conf);
      }
    }

    private FileSystem createFileSystem(URI uri, Configuration conf)
        throws IOException {
      final String fsImplConf = String.format(
          FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN,
          uri.getScheme());
      Class<?> clazz = conf.getClass(fsImplConf, null);
      if (clazz == null) {
        throw new UnsupportedFileSystemException(
            String.format("%s=null: %s: %s", fsImplConf,
                "No overload scheme fs configured", uri.getScheme()));
      }
      FileSystem fs = (FileSystem) newInstance(clazz, uri, conf);
      fs.initialize(uri, conf);
      return fs;
    }

    private <T> T newInstance(Class<T> theClass, URI uri, Configuration conf) {
      T result;
      try {
        Constructor<T> meth = theClass.getConstructor();
        meth.setAccessible(true);
        result = meth.newInstance();
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else {
          throw new RuntimeException(cause);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return result;
    }

  }

  /**
   * This is an admin only API to give access to its child raw file system, if
   * the path is link. If the given path is an internal directory(path is from
   * mount paths tree), it will initialize the file system of given path uri
   * directly. If path cannot be resolved to any internal directory or link, it
   * will throw NotInMountpointException. Please note, this API will not return
   * chrooted file system. Instead, this API will get actual raw file system
   * instances.
   *
   * @param path - fs uri path
   * @param conf - configuration
   * @throws IOException raised on errors performing I/O.
   * @return file system.
   */
  public FileSystem getRawFileSystem(Path path, Configuration conf)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(path), true);
      return res.isInternalDir() ? fsGetter().get(path.toUri(), conf)
          : ((ChRootedFileSystem) res.targetFileSystem).getMyFs();
    } catch (FileNotFoundException e) {
      // No link configured with passed path.
      throw new NotInMountpointException(path,
          "No link found for the given path.");
    }
  }

  /**
   * Gets the mount path info, which contains the target file system and
   * remaining path to pass to the target file system.
   *
   * @param path the path.
   * @param conf configuration.
   * @return mount path info.
   * @throws IOException raised on errors performing I/O.
   */
  public MountPathInfo<FileSystem> getMountPathInfo(Path path,
      Configuration conf) throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(path), true);
      FileSystem fs = res.isInternalDir() ?
          (fsState.getRootFallbackLink() != null ?
              fsState.getRootFallbackLink().getTargetFileSystem() :
              fsGetter().get(path.toUri(), conf)) :
          res.targetFileSystem;
      if (fs instanceof ChRootedFileSystem) {
        ChRootedFileSystem chFs = (ChRootedFileSystem) fs;
        return new MountPathInfo<>(chFs.fullPath(res.remainingPath),
            chFs.getMyFs());
      }
      return new MountPathInfo<FileSystem>(res.remainingPath, fs);
    } catch (FileNotFoundException e) {
      // No link configured with passed path.
      throw new NotInMountpointException(path,
          "No link found for the given path.");
    }
  }

  /**
   * A class to maintain the target file system and a path to pass to the target
   * file system.
   */
  public static class MountPathInfo<T> {
    private Path pathOnTarget;
    private T targetFs;

    public MountPathInfo(Path pathOnTarget, T targetFs) {
      this.pathOnTarget = pathOnTarget;
      this.targetFs = targetFs;
    }

    public Path getPathOnTarget() {
      return this.pathOnTarget;
    }

    public T getTargetFs() {
      return this.targetFs;
    }
  }

  /**
   * @return Gets the fallback file system configured. Usually, this will be the
   * default cluster.
   */
  public FileSystem getFallbackFileSystem() {
    if (fsState.getRootFallbackLink() == null) {
      return null;
    }
    try {
      return ((ChRootedFileSystem) fsState.getRootFallbackLink()
          .getTargetFileSystem()).getMyFs();
    } catch (IOException ex) {
      LOG.error("Could not get fallback filesystem ");
    }
    return null;
  }

  @Override
  @InterfaceAudience.LimitedPrivate("HDFS")
  public URI canonicalizeUri(URI uri) {
    return super.canonicalizeUri(uri);
  }
}