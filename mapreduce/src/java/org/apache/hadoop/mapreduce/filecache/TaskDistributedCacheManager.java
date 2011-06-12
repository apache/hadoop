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
package org.apache.hadoop.mapreduce.filecache;

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
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.filecache.TrackerDistributedCacheManager.CacheStatus;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Helper class of {@link TrackerDistributedCacheManager} that represents
 * the cached files of a single task.  This class is used
 * by TaskRunner/LocalJobRunner to parse out the job configuration
 * and setup the local caches.
 * 
 */
@InterfaceAudience.Private
public class TaskDistributedCacheManager {
  private final TrackerDistributedCacheManager distributedCacheManager;
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
    boolean isPublic = true;
    /** Whether to decompress */
    final FileType type;
    final long timestamp;
    /** Whether this is to be added to the classpath */
    final boolean shouldBeAddedToClassPath;
    boolean localized = false;
    /** The owner of the localized file. Relevant only on the tasktrackers */
    final String owner;
    private CacheStatus status;
    CacheFile(URI uri, FileType type, boolean isPublic, long timestamp, 
                      boolean classPath) throws IOException {
      this.uri = uri;
      this.type = type;
      this.isPublic = isPublic;
      this.timestamp = timestamp;
      this.shouldBeAddedToClassPath = classPath;
      this.owner =
          TrackerDistributedCacheManager.getLocalizedCacheOwner(isPublic);
    }

    /**
     * Set the status for this cache file.
     * @param status
     */
    public void setStatus(CacheStatus status) {
      this.status = status;
    }
    
    /**
     * Get the status for this cache file.
     * @return the status object
     */
    public CacheStatus getStatus() {
      return status;
    }

    /**
     * Converts the scheme used by DistributedCache to serialize what files to
     * cache in the configuration into CacheFile objects that represent those 
     * files.
     */
    private static List<CacheFile> makeCacheFiles(URI[] uris, 
        long[] timestamps, boolean cacheVisibilities[], Path[] paths, 
        FileType type) throws IOException {
      List<CacheFile> ret = new ArrayList<CacheFile>();
      if (uris != null) {
        if (uris.length != timestamps.length) {
          throw new IllegalArgumentException("Mismatched uris and timestamps.");
        }
        Map<String, Path> classPaths = new HashMap<String, Path>();
        if (paths != null) {
          for (Path p : paths) {
            classPaths.put(p.toUri().getPath().toString(), p);
          }
        }
        for (int i = 0; i < uris.length; ++i) {
          URI u = uris[i];
          boolean isClassPath = (null != classPaths.get(u.getPath()));
          ret.add(new CacheFile(u, type, cacheVisibilities[i],
              timestamps[i], isClassPath));
        }
      }
      return ret;
    }
    
    boolean getLocalized() {
      return localized;
    }
    
    void setLocalized(boolean val) {
      localized = val;
    }
  }

  TaskDistributedCacheManager(
      TrackerDistributedCacheManager distributedCacheManager,
      Configuration taskConf) throws IOException {
    this.distributedCacheManager = distributedCacheManager;
    
    this.cacheFiles.addAll(
        CacheFile.makeCacheFiles(DistributedCache.getCacheFiles(taskConf),
            DistributedCache.getFileTimestamps(taskConf),
            DistributedCache.getFileVisibilities(taskConf),
            DistributedCache.getFileClassPaths(taskConf),
            CacheFile.FileType.REGULAR));
    this.cacheFiles.addAll(
        CacheFile.makeCacheFiles(DistributedCache.getCacheArchives(taskConf),
          DistributedCache.getArchiveTimestamps(taskConf),
          DistributedCache.getArchiveVisibilities(taskConf),
          DistributedCache.getArchiveClassPaths(taskConf), 
          CacheFile.FileType.ARCHIVE));
  }

  /**
   * Retrieve public distributed cache files into the local cache and updates
   * the task configuration (which has been passed in via the constructor).
   * The private distributed cache is just looked at and the paths where the
   * files/archives should go to is decided here. The actual localization is
   * done by {@link JobLocalizer}.
   * 
   * It is the caller's responsibility to re-write the task configuration XML
   * file, if necessary.
   */
  public void setupCache(Configuration taskConf, String publicCacheSubdir, 
      String privateCacheSubdir) throws IOException {
    setupCalled = true;
      
    ArrayList<Path> localArchives = new ArrayList<Path>();
    ArrayList<Path> localFiles = new ArrayList<Path>();

    for (CacheFile cacheFile : cacheFiles) {
      URI uri = cacheFile.uri;
      FileSystem fileSystem = FileSystem.get(uri, taskConf);
      FileStatus fileStatus = fileSystem.getFileStatus(new Path(uri.getPath()));
      Path p;
      try {
        if (cacheFile.isPublic) {
          p = distributedCacheManager.getLocalCache(uri, taskConf,
              publicCacheSubdir, fileStatus, 
              cacheFile.type == CacheFile.FileType.ARCHIVE,
              cacheFile.timestamp, cacheFile.isPublic, cacheFile);
        } else {
          p = distributedCacheManager.getLocalCache(uri, taskConf,
              privateCacheSubdir, fileStatus, 
              cacheFile.type == CacheFile.FileType.ARCHIVE,
              cacheFile.timestamp, cacheFile.isPublic, cacheFile);
        }
      } catch (InterruptedException e) {
        throw new IOException("Interrupted localizing cache file", e);
      }
      cacheFile.setLocalized(true);

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
      // TODO verify
//      DistributedCache.addLocalArchives(taskConf, 
      DistributedCache.setLocalArchives(taskConf, 
        stringifyPathList(localArchives));
    }
    if (!localFiles.isEmpty()) {
      // TODO verify
//      DistributedCache.addLocalFiles(taskConf, stringifyPathList(localFiles));
      DistributedCache.setLocalFiles(taskConf,
        stringifyPathList(localFiles));
    }

  }

  /*
   * This method is called from unit tests.
   */
  List<CacheFile> getCacheFiles() {
    return cacheFiles;
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
      if (c.getLocalized() && c.status != null) {
        distributedCacheManager.releaseCache(c.status);
      }
    }
  }

  public void setSizes(long[] sizes) throws IOException {
    int i = 0;
    for (CacheFile c: cacheFiles) {
      if (!c.isPublic && c.type == CacheFile.FileType.ARCHIVE &&
    	  c.status != null) {
        distributedCacheManager.setSize(c.status, sizes[i++]);
      }
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
