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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

/******************************************************************************
 * This class is extended from the ViewFileSystem for the overloaded scheme 
 * file system. This object is the way end-user code interacts with a multiple
 * mounted file systems transparently. Overloaded scheme based uri can be
 * continued to use as end user interactive uri and mount links can be
 * configured to any Hadoop compatible file system. This class maintains all 
 * the target file system instances and delegates the calls to respective 
 * target file system based on mount link mapping. Mount link configuration
 * format and behavior is same as ViewFileSystem.
 *****************************************************************************/
@InterfaceAudience.LimitedPrivate({ "MapReduce", "HBase", "Hive" })
@InterfaceStability.Evolving
public class ViewFsOverloadScheme extends ViewFileSystem {

  public ViewFsOverloadScheme() throws IOException {
    super();
  }

  private FsCreator fsCreator;
  private String myScheme;

  @Override
  public String getScheme() {
    return myScheme;
  }

  @Override
  public void initialize(final URI theUri, final Configuration conf)
      throws IOException {
    superFSInit(theUri, conf);
    setConf(conf);
    config = conf;
    myScheme = config.get(FsConstants.VIEWFS_OVERLOAD_SCHEME_KEY);
    fsCreator = new FsCreator() {

      /**
       * This method is overridden because in ViewFsOverloadScheme if
       * overloaded scheme matches with mounted target fs scheme, file system
       * should be created without going into fs.<scheme>.impl based 
       * resolution. Otherwise it will end up into loop as target will be 
       * resolved again to ViewFsOverloadScheme as fs.<scheme>.impl points to
       * ViewFsOverloadScheme. So, below method will initialize the
       * fs.viewfs.overload.scheme.target.<scheme>.impl. Other schemes can
       * follow fs.newInstance
       */
      @Override
      public FileSystem createFs(URI uri, Configuration conf)
          throws IOException {
        if (uri.getScheme().equals(myScheme)) {
          // Avoid looping when target fs scheme is matching to overloaded
          // scheme.
          return createFileSystem(uri, conf);
        } else {
          return FileSystem.newInstance(uri, conf);
        }
      }

      private FileSystem createFileSystem(URI uri, Configuration conf)
          throws IOException {
        final String fsImplConf = String.format(
            FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN_KEY,
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

    final InnerCache innerCache = new InnerCache(fsCreator);

    // Now build client side view (i.e. client side mount table) from config.
    final String myAuthority = theUri.getAuthority();
    try {
      myUri = new URI(myScheme, myAuthority, "/", null,
          null);
      fsState = new InodeTree<FileSystem>(conf, myAuthority) {

        @Override
        protected FileSystem getTargetFileSystem(final URI uri)
            throws URISyntaxException, IOException {
          FileSystem fs = innerCache.get(uri, config);
          return new ChRootedFileSystem(fs, uri);
        }

        @Override
        protected FileSystem getTargetFileSystem(
            final INodeDir<FileSystem> dir)
            throws URISyntaxException {
          return new InternalDirOfViewFs(dir, creationTime, ugi, myUri,
              config);
        }

        @Override
        protected FileSystem getTargetFileSystem(final String settings,
            final URI[] uris) throws URISyntaxException, IOException {
          return NflyFSystem.createFileSystem(uris, config, settings);
        }
      };
      workingDir = this.getHomeDirectory();
      renameStrategy = RenameStrategy
          .valueOf(conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
              RenameStrategy.SAME_MOUNTPOINT.toString()));
    } catch (URISyntaxException e) {
      throw new IOException("URISyntax exception: " + theUri);
    }

    cache = innerCache.unmodifiableCache();
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (cache != null) {
      cache.closeAll();
    }
  }
}