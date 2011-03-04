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
package org.apache.hadoop.filecache;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;

/**
 * Helper class of {@link TrackerDistributedCacheManager} that represents
 * the cached files of a single task.  This class is used
 * by TaskRunner/LocalJobRunner to parse out the job configuration
 * and setup the local caches.
 * 
 * <b>This class is internal to Hadoop, and should not be treated as a public
 * interface.</b>
 */
public class TaskDistributedCacheManager {
  private final TrackerDistributedCacheManager distributedCacheManager;
  private final Configuration taskConf;
  private final List<CacheFile> cacheFiles = new ArrayList<CacheFile>();
  private final List<String> classPaths = new ArrayList<String>();
 
  private boolean setupCalled = false;

  /**
   * Struct representing a single cached file.
   * There are four permutations (archive, file) and
   * (don't put in classpath, do put in classpath).
   */
  static class CacheFile {
    /** URI as in the configuration */
    final URI uri;
    enum FileType {
      REGULAR,
      ARCHIVE
    }
    /** Whether to decompress */
    final FileType type;
    final long timestamp;
    /** Whether this is to be added to the classpath */
    final boolean shouldBeAddedToClassPath;

    private CacheFile(URI uri, FileType type, long timestamp, 
        boolean classPath) {
      this.uri = uri;
      this.type = type;
      this.timestamp = timestamp;
      this.shouldBeAddedToClassPath = classPath;
    }

    /**
     * Converts the scheme used by DistributedCache to serialize what files to
     * cache in the configuration into CacheFile objects that represent those 
     * files.
     */
    private static List<CacheFile> makeCacheFiles(URI[] uris, 
        String[] timestamps, Path[] paths, FileType type) {
      List<CacheFile> ret = new ArrayList<CacheFile>();
      if (uris != null) {
        if (uris.length != timestamps.length) {
          throw new IllegalArgumentException("Mismatched uris and timestamps.");
        }
        Map<String, Path> classPaths = new HashMap<String, Path>();
        if (paths != null) {
          for (Path p : paths) {
            classPaths.put(p.toString(), p);
          }
        }
        for (int i = 0; i < uris.length; ++i) {
          URI u = uris[i];
          boolean isClassPath = (null != classPaths.get(u.getPath()));
          long t = Long.parseLong(timestamps[i]);
          ret.add(new CacheFile(u, type, t, isClassPath));
        }
      }
      return ret;
    }
  }

  TaskDistributedCacheManager(
      TrackerDistributedCacheManager distributedCacheManager,
      Configuration taskConf) throws IOException {
    this.distributedCacheManager = distributedCacheManager;
    this.taskConf = taskConf;
    
    this.cacheFiles.addAll(
        CacheFile.makeCacheFiles(DistributedCache.getCacheFiles(taskConf),
            DistributedCache.getFileTimestamps(taskConf),
            DistributedCache.getFileClassPaths(taskConf),
            CacheFile.FileType.REGULAR));
    this.cacheFiles.addAll(
        CacheFile.makeCacheFiles(DistributedCache.getCacheArchives(taskConf),
          DistributedCache.getArchiveTimestamps(taskConf),
          DistributedCache.getArchiveClassPaths(taskConf), 
          CacheFile.FileType.ARCHIVE));
  }

  /**
   * Retrieve files into the local cache and updates the task configuration 
   * (which has been passed in via the constructor).
   * 
   * It is the caller's responsibility to re-write the task configuration XML
   * file, if necessary.
   */
  public void setup(LocalDirAllocator lDirAlloc, File workDir, 
      String cacheSubdir) throws IOException {
    setupCalled = true;
    
    if (cacheFiles.isEmpty()) {
      return;
    }

    ArrayList<Path> localArchives = new ArrayList<Path>();
    ArrayList<Path> localFiles = new ArrayList<Path>();
    Path workdirPath = new Path(workDir.getAbsolutePath());

    for (CacheFile cacheFile : cacheFiles) {
      URI uri = cacheFile.uri;
      FileSystem fileSystem = FileSystem.get(uri, taskConf);
      FileStatus fileStatus = fileSystem.getFileStatus(new Path(uri.getPath()));
      String cacheId = this.distributedCacheManager.makeRelative(uri, taskConf);
      String cachePath = cacheSubdir + Path.SEPARATOR + cacheId;
      Path localPath = lDirAlloc.getLocalPathForWrite(cachePath,
                                fileStatus.getLen(), taskConf);
      String baseDir = localPath.toString().replace(cacheId, "");
      Path p = distributedCacheManager.getLocalCache(uri, taskConf,
          new Path(baseDir), fileStatus, 
          cacheFile.type == CacheFile.FileType.ARCHIVE,
          cacheFile.timestamp, workdirPath, false);

      if (cacheFile.type == CacheFile.FileType.ARCHIVE) {
        localArchives.add(p);
      } else {
        localFiles.add(p);
      }
      if (cacheFile.shouldBeAddedToClassPath) {
        classPaths.add(p.toString());
      }
    }

    // Update the configuration object with localized data.
    if (!localArchives.isEmpty()) {
      DistributedCache.setLocalArchives(taskConf, 
        stringifyPathList(localArchives));
    }
    if (!localFiles.isEmpty()) {
      DistributedCache.setLocalFiles(taskConf, stringifyPathList(localFiles));
    }

  }

  private static String stringifyPathList(List<Path> p){
    if (p == null || p.isEmpty()) {
      return null;
    }
    StringBuilder str = new StringBuilder(p.get(0).toString());
    for (int i = 1; i < p.size(); i++){
      str.append(",");
      str.append(p.get(i).toString());
    }
    return str.toString();
  }

  /** 
   * Retrieves class paths (as local references) to add. 
   * Should be called after setup().
   * 
   */
  public List<String> getClassPaths() throws IOException {
    if (!setupCalled) {
      throw new IllegalStateException(
          "getClassPaths() should be called after setup()");
    }
    return classPaths;
  }

  /**
   * Releases the cached files/archives, so that space
   * can be reclaimed by the {@link TrackerDistributedCacheManager}.
   */
  public void release() throws IOException {
    for (CacheFile c : cacheFiles) {
      distributedCacheManager.releaseCache(c.uri, taskConf);
    }
  }

  /**
   * Creates a class loader that includes the designated
   * files and archives.
   */
  public ClassLoader makeClassLoader(final ClassLoader parent) 
      throws MalformedURLException {
    final URL[] urls = new URL[classPaths.size()];
    for (int i = 0; i < classPaths.size(); ++i) {
      urls[i] = new File(classPaths.get(i)).toURI().toURL();
    }
    return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
      @Override
      public ClassLoader run() {
        return new URLClassLoader(urls, parent);
      }     
    });
  }
}
