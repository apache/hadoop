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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME;

/******************************************************************************
 * This class is AbstractFileSystem implementation corresponding to
 * ViewFileSystemOverloadScheme. This class is extended from the ViewFs
 * for the overloaded scheme file system. Mount link configurations and
 * in-memory mount table building behaviors are inherited from ViewFs.
 * Unlike ViewFs scheme (viewfs://), the users would be able to use any scheme.
 *
 * To use this class, the following configurations need to be added in
 * core-site.xml file.
 * 1) fs.AbstractFileSystem.<scheme>.impl
 *    = org.apache.hadoop.fs.viewfs.ViewFsOverloadScheme
 * 2) fs.viewfs.overload.scheme.target.abstract.<scheme>.impl
 *    = <hadoop compatible file system implementation class name for the
 *    <scheme>"
 *
 * Here <scheme> can be any scheme, but with that scheme there should be a
 * hadoop compatible file system available. Second configuration value should
 * be the respective scheme's file system implementation class.
 * Example: if scheme is configured with "hdfs", then the 2nd configuration
 * class name will be org.apache.hadoop.hdfs.Hdfs.
 *
 * Use Case 1:
 * ===========
 * If users want some of their existing cluster (hdfs://Cluster)
 * data to mount with other hdfs and object store clusters(hdfs://NN1,
 * o3fs://bucket1.volume1/, s3a://bucket1/)
 *
 * fs.viewfs.mounttable.Cluster.link./user = hdfs://NN1/user
 * fs.viewfs.mounttable.Cluster.link./data = o3fs://bucket1.volume1/data
 * fs.viewfs.mounttable.Cluster.link./backup = s3a://bucket1/backup/
 *
 * Op1: Create file hdfs://Cluster/user/fileA will go to hdfs://NN1/user/fileA
 * Op2: Create file hdfs://Cluster/data/datafile will go to
 *      o3fs://bucket1.volume1/data/datafile
 * Op3: Create file hdfs://Cluster/backup/data.zip will go to
 *      s3a://bucket1/backup/data.zip
 *
 * Use Case 2:
 * ===========
 * If users want some of their existing cluster (s3a://bucketA/)
 * data to mount with other hdfs and object store clusters
 * (hdfs://NN1, o3fs://bucket1.volume1/)
 *
 * fs.viewfs.mounttable.bucketA.link./user = hdfs://NN1/user
 * fs.viewfs.mounttable.bucketA.link./data = o3fs://bucket1.volume1/data
 * fs.viewfs.mounttable.bucketA.link./salesDB = s3a://bucketA/salesDB/
 *
 * Op1: Create file s3a://bucketA/user/fileA will go to hdfs://NN1/user/fileA
 * Op2: Create file s3a://bucketA/data/datafile will go to
 *      o3fs://bucket1.volume1/data/datafile
 * Op3: Create file s3a://bucketA/salesDB/dbfile will go to
 *      s3a://bucketA/salesDB/dbfile
 *
 * Note:
 * (1) In ViewFsOverloadScheme, by default the mount links will be
 * represented as non-symlinks. If you want to change this behavior, please see
 * {@link ViewFs#listStatus(Path)}
 * (2) In ViewFsOverloadScheme, only the initialized uri's hostname will
 * be considered as the mount table name. When the passed uri has hostname:port,
 * it will simply ignore the port number and only hostname will be considered as
 * the mount table name.
 * (3) If there are no mount links configured with the initializing uri's
 * hostname as the mount table name, then it will automatically consider the
 * current uri as fallback( ex: fs.viewfs.mounttable.<mycluster>.linkFallBack)
 * target fs uri.
 *****************************************************************************/

public class ViewFsOverloadScheme extends ViewFs {
  private URI myUri;
  private static final Class<?>[] URI_CONFIG_ARGS =
      new Class[]{URI.class, Configuration.class};

  public ViewFsOverloadScheme() throws IOException, URISyntaxException {
    super(new Configuration());
  }

  public ViewFsOverloadScheme(final URI theUri, final Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, conf);
  }

  @Override
  void loadMountTable(URI theUri, Configuration conf,
      boolean initingUriAsFallbackOnNoMounts)
      throws URISyntaxException, IOException {
    myUri = theUri;
    /* The default value to false in ViewFSOverloadScheme */
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS,
        conf.getBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS,
            false));
    /* the default value to true in ViewFSOverloadScheme */
    conf.setBoolean(CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME,
        conf.getBoolean(Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME,
            true));
    loadMountTableConfigs(theUri, conf);
    super.loadMountTable(theUri, conf, initingUriAsFallbackOnNoMounts);
  }

  protected void loadMountTableConfigs(URI uri, Configuration conf)
      throws IOException {
    String mountTableConfigPath =
        conf.get(Constants.CONFIG_VIEWFS_MOUNTTABLE_PATH);
    if (null != mountTableConfigPath) {
      MountTableConfigLoader loader = new HCFSMountTableConfigLoader();
      loader.load(mountTableConfigPath, conf);
    } else {
      // TODO: Should we fail here.?
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Missing configuration for fs.viewfs.mounttable.path. Proceeding"
                + "with core-site.xml mount-table information if available.");
      }
    }
  }

  @Override
  protected AbstractFsGetter fsGetter() {
    return new ChildAbstractFsGetter(myUri.getScheme());
  }

  /**
   * This class checks whether the rootScheme is same as URI scheme. If both are
   * same, then it will initialize file systems by using the configured
   * fs.viewfs.overload.scheme.target.abstract.<scheme>.impl class.
   */
  static class ChildAbstractFsGetter extends AbstractFsGetter {

    private final String rootScheme;

    ChildAbstractFsGetter(String rootScheme) {
      this.rootScheme = rootScheme;
    }

    @Override
    public AbstractFileSystem getNewInstance(URI uri, Configuration conf)
        throws UnsupportedFileSystemException {
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
        return AbstractFileSystem.createFileSystem(uri, conf);
      }
    }

    private AbstractFileSystem createFileSystem(URI uri, Configuration conf)
        throws UnsupportedFileSystemException {
      final String fsImplConf = String.format(
          FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_ABSTRACTFS_IMPL_PATTERN,
          uri.getScheme());
      Class<?> clazz = conf.getClass(fsImplConf, null);
      if (clazz == null) {
        throw new UnsupportedFileSystemException(
            String.format("%s=null: %s: %s", fsImplConf,
                "No overload scheme fs configured", uri.getScheme()));
      }
      return (AbstractFileSystem) newInstance(clazz, uri, conf);
    }

    private <T> T newInstance(Class<T> theClass, URI uri, Configuration conf) {
      T result;
      try {
        Constructor<T> meth = theClass.getDeclaredConstructor(URI_CONFIG_ARGS);
        meth.setAccessible(true);
        result = meth.newInstance(uri, conf);
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
}
