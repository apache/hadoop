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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

/******************************************************************************
 * This class is extended from the ViewFileSystem for the overloaded scheme file
 * system. The objective here is to handle multiple mounted file systems
 * transparently. Mount link configurations and in-memory mount table
 * building behaviors are inherited from ViewFileSystem. Unlike ViewFileSystem
 * scheme (viewfs://), the users would be able to use any scheme.
 *
 * Example 1:
 * If users want some of their existing cluster (hdfs://Cluster)
 * data to mount with other hdfs and object store clusters(hdfs://NN1,
 * o3fs://bucket1.volume1/, s3a://bucket1/)
 *
 * fs.viewfs.mounttable.Cluster./user = hdfs://NN1/user
 * fs.viewfs.mounttable.Cluster./data = o3fs://bucket1.volume1/data
 * fs.viewfs.mounttable.Cluster./backup = s3a://bucket1/backup/
 *
 * Op1: Create file hdfs://Cluster/user/fileA will go to hdfs://NN1/user/fileA
 * Op2: Create file hdfs://Cluster/data/datafile will go to
 *      o3fs://bucket1.volume1/data/datafile
 * Op3: Create file hdfs://Cluster/backup/data.zip will go to
 *      s3a://bucket1/backup/data.zip
 *
 * Example 2:
 * If users want some of their existing cluster (s3a://bucketA/)
 * data to mount with other hdfs and object store clusters
 * (hdfs://NN1, o3fs://bucket1.volume1/)
 *
 * fs.viewfs.mounttable.bucketA./user = hdfs://NN1/user
 * fs.viewfs.mounttable.bucketA./data = o3fs://bucket1.volume1/data
 * fs.viewfs.mounttable.bucketA./salesDB = s3a://bucketA/salesDB/
 *
 * Op1: Create file s3a://bucketA/user/fileA will go to hdfs://NN1/user/fileA
 * Op2: Create file s3a://bucketA/data/datafile will go to
 *      o3fs://bucket1.volume1/data/datafile
 * Op3: Create file s3a://bucketA/salesDB/dbfile will go to
 *      s3a://bucketA/salesDB/dbfile
 *****************************************************************************/
@InterfaceAudience.LimitedPrivate({ "MapReduce", "HBase", "Hive" })
@InterfaceStability.Evolving
public class ViewFsOverloadScheme extends ViewFileSystem {
  private URI myUri;
  public ViewFsOverloadScheme() throws IOException {
    super();
  }

  @Override
  public String getScheme() {
    return myUri.getScheme();
  }

  @Override
  public void initialize(URI theUri, Configuration conf) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Initializing the ViewFsOverloadScheme with the uri: " + theUri);
    }
    this.myUri = theUri;
    super.initialize(theUri, conf);
  }

  /**
   * This method is overridden because in ViewFsOverloadScheme if overloaded
   * scheme matches with mounted target fs scheme, file system should be
   * created without going into fs.<scheme>.impl based resolution. Otherwise
   * it will end up in an infinite loop as the target will be resolved again
   * to ViewFsOverloadScheme as fs.<scheme>.impl points to
   * ViewFsOverloadScheme. So, below method will initialize the
   * fs.viewfs.overload.scheme.target.<scheme>.impl. Other schemes can
   * follow fs.newInstance
   */
  @Override
  protected FsGetter fsGetter() {

    return new FsGetter() {
      @Override
      public FileSystem getNewInstance(URI uri, Configuration conf)
          throws IOException {
        if (uri.getScheme().equals(getScheme())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "The file system initialized uri scheme is matching with the "
                    + "given target uri scheme. The target uri is: " + uri);
          }
          /*
           * Avoid looping when target fs scheme is matching to overloaded
           * scheme.
           */
          return createFileSystem(uri, conf);
        } else {
          return FileSystem.newInstance(uri, conf);
        }
      }

      /**
       * When ViewFSOverloadScheme scheme and target uri scheme are matching,
       * it will not take advantage of FileSystem cache as it will create
       * instance directly. For caching needs please set
       * "fs.viewfs.enable.inner.cache" to true.
       */
      @Override
      public FileSystem get(URI uri, Configuration conf) throws IOException {
        if (uri.getScheme().equals(getScheme())) {
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

      private <T> T newInstance(Class<T> theClass, URI uri,
          Configuration conf) {
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
    };
  }
}