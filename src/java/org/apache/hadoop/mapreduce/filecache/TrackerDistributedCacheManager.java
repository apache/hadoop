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
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskController.DistributedCacheFileContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Manages a single machine's instance of a cross-job
 * cache.  This class would typically be instantiated
 * by a TaskTracker (or something that emulates it,
 * like LocalJobRunner).
 * 
 */
@InterfaceAudience.Private
public class TrackerDistributedCacheManager {
  // cacheID to cacheStatus mapping
  private TreeMap<String, CacheStatus> cachedArchives = 
    new TreeMap<String, CacheStatus>();

  private TreeMap<Path, Long> baseDirSize = new TreeMap<Path, Long>();

  // default total cache size (10GB)
  private static final long DEFAULT_CACHE_SIZE = 10737418240L;

  private static final Log LOG =
    LogFactory.getLog(TrackerDistributedCacheManager.class);

  private final LocalFileSystem localFs;
  
  private LocalDirAllocator lDirAllocator;
  
  private TaskController taskController;
  
  private Configuration trackerConf;
  
  private Random random = new Random();

  private MRAsyncDiskService asyncDiskService;
  
  public TrackerDistributedCacheManager(Configuration conf,
      TaskController taskController) throws IOException {
    this.localFs = FileSystem.getLocal(conf);
    this.trackerConf = conf;
    this.lDirAllocator = new LocalDirAllocator(TTConfig.LOCAL_DIR);
    this.taskController = taskController;
  }

  /**
   * Creates a TrackerDistributedCacheManager with a MRAsyncDiskService.
   * @param asyncDiskService Provides a set of ThreadPools for async disk 
   *                         operations.  
   */
  public TrackerDistributedCacheManager(Configuration conf,
      TaskController taskController, MRAsyncDiskService asyncDiskService)
      throws IOException {
    this(conf, taskController);
    this.asyncDiskService = asyncDiskService;
  }

  /**
   * Get the locally cached file or archive; it could either be
   * previously cached (and valid) or copy it from the {@link FileSystem} now.
   *
   * @param cache the cache to be localized, this should be specified as
   * new URI(scheme://scheme-specific-part/absolute_path_to_file#LINKNAME).
   * @param conf The Configuration file which contains the filesystem
   * @param subDir The base cache subDir where you want to localize the 
   *  files/archives
   * @param fileStatus The file status on the dfs.
   * @param isArchive if the cache is an archive or a file. In case it is an
   *  archive with a .zip or .jar or .tar or .tgz or .tar.gz extension it will
   *  be unzipped/unjarred/untarred automatically
   *  and the directory where the archive is unzipped/unjarred/untarred is
   *  returned as the Path.
   *  In case of a file, the path to the file is returned
   * @param confFileStamp this is the hdfs file modification timestamp to verify
   * that the file to be cached hasn't changed since the job started
   * @param currentWorkDir this is the directory where you would want to create
   * symlinks for the locally cached files/archives
   * @param honorSymLinkConf if this is false, then the symlinks are not
   * created even if conf says so (this is required for an optimization in task
   * launches
   * NOTE: This is effectively always on since r696957, since there is no code
   * path that does not use this.
   * @param isPublic to know the cache file is accessible to public or private
   * @return the path to directory where the archives are unjarred in case of
   * archives, the path to the file where the file is copied locally
   * @throws IOException
   */
  Path getLocalCache(URI cache, Configuration conf,
      String subDir, FileStatus fileStatus,
      boolean isArchive, long confFileStamp,
      Path currentWorkDir, boolean honorSymLinkConf, boolean isPublic)
      throws IOException {
    String key = getKey(cache, conf, confFileStamp);
    CacheStatus lcacheStatus;
    Path localizedPath = null;
    synchronized (cachedArchives) {
      lcacheStatus = cachedArchives.get(key);
      if (lcacheStatus == null) {
        // was never localized
        String uniqueString = String.valueOf(random.nextLong());
        String cachePath = new Path (subDir, 
          new Path(uniqueString, makeRelative(cache, conf))).toString();
        Path localPath = lDirAllocator.getLocalPathForWrite(cachePath,
          fileStatus.getLen(), trackerConf);
        lcacheStatus = new CacheStatus(new Path(localPath.toString().replace(
          cachePath, "")), localPath, new Path(subDir), uniqueString);
        cachedArchives.put(key, lcacheStatus);
      }

      //mark the cache for use. 
      lcacheStatus.refcount++;
    }
    
    boolean initSuccessful = false;
    try {
      // do the localization, after releasing the global lock
      synchronized (lcacheStatus) {
        if (!lcacheStatus.isInited()) {
          localizedPath = localizeCache(conf, cache, confFileStamp,
              lcacheStatus, fileStatus, isArchive, isPublic);
          lcacheStatus.initComplete();
        } else {
          localizedPath = checkCacheStatusValidity(conf, cache, confFileStamp,
              lcacheStatus, fileStatus, isArchive);
        }
        createSymlink(conf, cache, lcacheStatus, isArchive, currentWorkDir,
            honorSymLinkConf);
      }

      // try deleting stuff if you can
      long size = 0;
      synchronized (lcacheStatus) {
        synchronized (baseDirSize) {
          Long get = baseDirSize.get(lcacheStatus.getBaseDir());
          if (get != null) {
            size = get.longValue();
          } else {
            LOG.warn("Cannot find size of baseDir: "
                + lcacheStatus.getBaseDir());
          }
        }
      }
      // setting the cache size to a default of 10GB
      long allowedSize = conf.getLong(TTConfig.TT_LOCAL_CACHE_SIZE,
          DEFAULT_CACHE_SIZE);
      if (allowedSize < size) {
        // try some cache deletions
        deleteCache(conf);
      }
      initSuccessful = true;
      return localizedPath;
    } finally {
      if (!initSuccessful) {
        synchronized (cachedArchives) {
          lcacheStatus.refcount--;
        }
      }
    }
  }

  /**
   * This is the opposite of getlocalcache. When you are done with
   * using the cache, you need to release the cache
   * @param cache The cache URI to be released
   * @param conf configuration which contains the filesystem the cache
   * is contained in.
   * @throws IOException
   */
  void releaseCache(URI cache, Configuration conf, long timeStamp)
    throws IOException {
    String key = getKey(cache, conf, timeStamp);
    synchronized (cachedArchives) {
      CacheStatus lcacheStatus = cachedArchives.get(key);
      if (lcacheStatus == null) {
        LOG.warn("Cannot find localized cache: " + cache + 
                 " (key: " + key + ") in releaseCache!");
        return;
      }
      
      // decrement ref count 
      lcacheStatus.refcount--;
    }
  }

  /*
   * This method is called from unit tests. 
   */
  int getReferenceCount(URI cache, Configuration conf, long timeStamp) 
    throws IOException {
    String key = getKey(cache, conf, timeStamp);
    synchronized (cachedArchives) {
      CacheStatus lcacheStatus = cachedArchives.get(key);
      if (lcacheStatus == null) {
        throw new IOException("Cannot find localized cache: " + cache);
      }
      return lcacheStatus.refcount;
    }
  }

  // To delete the caches which have a refcount of zero

  private void deleteCache(Configuration conf) throws IOException {
    Set<CacheStatus> deleteSet = new HashSet<CacheStatus>();
    // try deleting cache Status with refcount of zero
    synchronized (cachedArchives) {
      for (Iterator<String> it = cachedArchives.keySet().iterator(); 
          it.hasNext();) {
        String cacheId = it.next();
        CacheStatus lcacheStatus = cachedArchives.get(cacheId);
        
        // if reference count is zero 
        // mark the cache for deletion
        if (lcacheStatus.refcount == 0) {
          // delete this cache entry from the global list 
          // and mark the localized file for deletion
          deleteSet.add(lcacheStatus);
          it.remove();
        }
      }
    }
    
    // do the deletion, after releasing the global lock
    for (CacheStatus lcacheStatus : deleteSet) {
      synchronized (lcacheStatus) {
        deleteLocalPath(asyncDiskService,
            FileSystem.getLocal(conf), lcacheStatus.localizedLoadPath);
        // decrement the size of the cache from baseDirSize
        synchronized (baseDirSize) {
          Long dirSize = baseDirSize.get(lcacheStatus.localizedBaseDir);
          if ( dirSize != null ) {
            dirSize -= lcacheStatus.size;
            baseDirSize.put(lcacheStatus.localizedBaseDir, dirSize);
          } else {
            LOG.warn("Cannot find record of the baseDir: " + 
                     lcacheStatus.localizedBaseDir + " during delete!");
          }
        }
      }
    }
  }

  /**
   * Delete a local path with asyncDiskService if available,
   * or otherwise synchronously with local file system.
   */
  private static void deleteLocalPath(MRAsyncDiskService asyncDiskService,
      LocalFileSystem fs, Path path) throws IOException {
    boolean deleted = false;
    if (asyncDiskService != null) {
      // Try to delete using asyncDiskService
      String localPathToDelete = 
        path.toUri().getPath();
      deleted = asyncDiskService.moveAndDeleteAbsolutePath(localPathToDelete);
      if (!deleted) {
        LOG.warn("Cannot find DistributedCache path " + localPathToDelete
            + " on any of the asyncDiskService volumes!");
      }
    }
    if (!deleted) {
      // If no asyncDiskService, we will delete the files synchronously
      fs.delete(path, true);
    }
    LOG.info("Deleted path " + path);
  }
  
  /*
   * Returns the relative path of the dir this cache will be localized in
   * relative path that this cache will be localized in. For
   * hdfs://hostname:port/absolute_path -- the relative path is
   * hostname/absolute path -- if it is just /absolute_path -- then the
   * relative path is hostname of DFS this mapred cluster is running
   * on/absolute_path
   */
  String makeRelative(URI cache, Configuration conf)
    throws IOException {
    String host = cache.getHost();
    if (host == null) {
      host = cache.getScheme();
    }
    if (host == null) {
      URI defaultUri = FileSystem.get(conf).getUri();
      host = defaultUri.getHost();
      if (host == null) {
        host = defaultUri.getScheme();
      }
    }
    String path = host + cache.getPath();
    path = path.replace(":/","/");                // remove windows device colon
    return path;
  }

  String getKey(URI cache, Configuration conf, long timeStamp) 
      throws IOException {
    return makeRelative(cache, conf) + String.valueOf(timeStamp);
  }
  
  /**
   * Returns mtime of a given cache file on hdfs.
   * 
   * @param conf configuration
   * @param cache cache file 
   * @return mtime of a given cache file on hdfs
   * @throws IOException
   */
  static long getTimestamp(Configuration conf, URI cache)
    throws IOException {
    FileSystem fileSystem = FileSystem.get(cache, conf);
    Path filePath = new Path(cache.getPath());

    return fileSystem.getFileStatus(filePath).getModificationTime();
  }
  
  /**
   * Returns a boolean to denote whether a cache file is visible to all(public)
   * or not
   * @param conf
   * @param uri
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException
   */
  static boolean isPublic(Configuration conf, URI uri) throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path current = new Path(uri.getPath());
    //the leaf level file should be readable by others
    if (!checkPermissionOfOther(fs, current, FsAction.READ)) {
      return false;
    }
    current = current.getParent();
    while (current != null) {
      //the subdirs in the path should have execute permissions for others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE)) {
        return false;
      }
      current = current.getParent();
    }
    return true;
  }
  /**
   * Checks for a given path whether the Other permissions on it 
   * imply the permission in the passed FsAction
   * @param fs
   * @param path
   * @param action
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException
   */
  private static boolean checkPermissionOfOther(FileSystem fs, Path path,
      FsAction action) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    FsPermission perms = status.getPermission();
    FsAction otherAction = perms.getOtherAction();
    if (otherAction.implies(action)) {
      return true;
    }
    return false;
  }

  private Path checkCacheStatusValidity(Configuration conf,
      URI cache, long confFileStamp,
      CacheStatus cacheStatus,
      FileStatus fileStatus,
      boolean isArchive
      ) throws IOException {
    FileSystem fs = FileSystem.get(cache, conf);
    // Has to be 
    if (!ifExistsAndFresh(conf, fs, cache, confFileStamp,
                          cacheStatus, fileStatus)) {
      throw new IOException("Stale cache file: " + cacheStatus.localizedLoadPath + 
                            " for cache-file: " + cache);
    }

    LOG.info(String.format("Using existing cache of %s->%s",
        cache.toString(), cacheStatus.localizedLoadPath));
    return cacheStatus.localizedLoadPath;
  }
  
  private void createSymlink(Configuration conf, URI cache,
      CacheStatus cacheStatus, boolean isArchive,
      Path currentWorkDir, boolean honorSymLinkConf) throws IOException {
    boolean doSymlink = honorSymLinkConf && DistributedCache.getSymlink(conf);
    if(cache.getFragment() == null) {
      doSymlink = false;
    }
    String link = 
      currentWorkDir.toString() + Path.SEPARATOR + cache.getFragment();
    File flink = new File(link);
    if (doSymlink){
      if (!flink.exists()) {
        FileUtil.symLink(cacheStatus.localizedLoadPath.toString(), link);
      }
    }
  }
  
  // the method which actually copies the caches locally and unjars/unzips them
  // and does chmod for the files
  Path localizeCache(Configuration conf,
                                    URI cache, long confFileStamp,
                                    CacheStatus cacheStatus,
                                    FileStatus fileStatus,
                                    boolean isArchive, boolean isPublic)
  throws IOException {
    FileSystem fs = FileSystem.get(cache, conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    Path parchive = null;
    if (isArchive) {
      parchive = new Path(cacheStatus.localizedLoadPath,
        new Path(cacheStatus.localizedLoadPath.getName()));
    } else {
      parchive = cacheStatus.localizedLoadPath;
    }

    if (!localFs.mkdirs(parchive.getParent())) {
      throw new IOException("Mkdirs failed to create directory " +
          cacheStatus.localizedLoadPath.toString());
    }

    String cacheId = cache.getPath();
    fs.copyToLocalFile(new Path(cacheId), parchive);
    if (isArchive) {
      String tmpArchive = parchive.toString().toLowerCase();
      File srcFile = new File(parchive.toString());
      File destDir = new File(parchive.getParent().toString());
      LOG.info(String.format("Extracting %s to %s",
          srcFile.toString(), destDir.toString()));
      if (tmpArchive.endsWith(".jar")) {
        RunJar.unJar(srcFile, destDir);
      } else if (tmpArchive.endsWith(".zip")) {
        FileUtil.unZip(srcFile, destDir);
      } else if (isTarFile(tmpArchive)) {
        FileUtil.unTar(srcFile, destDir);
      } else {
        LOG.warn(String.format(
            "Cache file %s specified as archive, but not valid extension.", 
            srcFile.toString()));
        // else will not do anyhting
        // and copy the file into the dir as it is
      }
    }

    long cacheSize = 
      FileUtil.getDU(new File(parchive.getParent().toString()));
    cacheStatus.size = cacheSize;
    synchronized (baseDirSize) {
      Long dirSize = baseDirSize.get(cacheStatus.localizedBaseDir);
      if( dirSize == null ) {
        dirSize = Long.valueOf(cacheSize);
      } else {
        dirSize += cacheSize;
      }
      baseDirSize.put(cacheStatus.localizedBaseDir, dirSize);
    }

    // set proper permissions for the localized directory
    setPermissions(conf, cacheStatus, isPublic);

    // update cacheStatus to reflect the newly cached file
    cacheStatus.mtime = getTimestamp(conf, cache);

    LOG.info(String.format("Cached %s as %s",
             cache.toString(), cacheStatus.localizedLoadPath));
    return cacheStatus.localizedLoadPath;
  }

  private void setPermissions(Configuration conf, CacheStatus cacheStatus,
      boolean isPublic) throws IOException {
    if (isPublic) {
      Path localizedUniqueDir = cacheStatus.getLocalizedUniqueDir();
      LOG.info("Doing chmod on localdir :" + localizedUniqueDir);
      try {
        FileUtil.chmod(localizedUniqueDir.toString(), "ugo+rx", true);
      } catch (InterruptedException e) {
        LOG.warn("Exception in chmod" + e.toString());
        throw new IOException(e);
      }
    } else {
      // invoke taskcontroller to set permissions
      DistributedCacheFileContext context = new DistributedCacheFileContext(
          conf.get(JobContext.USER_NAME), new File(cacheStatus.localizedBaseDir
              .toString()), cacheStatus.localizedBaseDir,
          cacheStatus.uniqueString);
      taskController.initializeDistributedCacheFile(context);
    }
  }

  private static boolean isTarFile(String filename) {
    return (filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
           filename.endsWith(".tar"));
  }

  // Checks if the cache has already been localized and is fresh
  private boolean ifExistsAndFresh(Configuration conf, FileSystem fs,
                                          URI cache, long confFileStamp,
                                          CacheStatus lcacheStatus,
                                          FileStatus fileStatus)
  throws IOException {
    long dfsFileStamp;
    if (fileStatus != null) {
      dfsFileStamp = fileStatus.getModificationTime();
    } else {
      dfsFileStamp = getTimestamp(conf, cache);
    }

    // ensure that the file on hdfs hasn't been modified since the job started
    if (dfsFileStamp != confFileStamp) {
      LOG.fatal("File: " + cache + " has changed on HDFS since job started");
      throw new IOException("File: " + cache +
                            " has changed on HDFS since job started");
    }

    if (dfsFileStamp != lcacheStatus.mtime) {
      return false;
    }

    return true;
  }

  /**
   * This method create symlinks for all files in a given dir in another 
   * directory.
   * 
   * Should not be used outside of DistributedCache code.
   * 
   * @param conf the configuration
   * @param jobCacheDir the target directory for creating symlinks
   * @param workDir the directory in which the symlinks are created
   * @throws IOException
   */
  public static void createAllSymlink(Configuration conf, File jobCacheDir, 
      File workDir)
    throws IOException{
    if ((jobCacheDir == null || !jobCacheDir.isDirectory()) ||
           workDir == null || (!workDir.isDirectory())) {
      return;
    }
    boolean createSymlink = DistributedCache.getSymlink(conf);
    if (createSymlink){
      File[] list = jobCacheDir.listFiles();
      for (int i=0; i < list.length; i++){
        String target = list[i].getAbsolutePath();
        String link = new File(workDir, list[i].getName()).toString();
        LOG.info(String.format("Creating symlink: %s <- %s", target, link));
        int ret = FileUtil.symLink(target, link);
        if (ret != 0) {
          LOG.warn(String.format("Failed to create symlink: %s <- %s", target,
              link));
        }
      }
    }
  }

  static class CacheStatus {
    // the local load path of this cache
    Path localizedLoadPath;

    //the base dir where the cache lies
    Path localizedBaseDir;

    //the size of this cache
    long size;

    // number of instances using this cache
    int refcount;

    // the cache-file modification time
    long mtime;

    // is it initialized ?
    boolean inited = false;

    // The sub directory (tasktracker/archive or tasktracker/user/archive),
    // under which the file will be localized
    Path subDir;
    
    // unique string used in the construction of local load path
    String uniqueString;

    public CacheStatus(Path baseDir, Path localLoadPath, Path subDir,
        String uniqueString) {
      super();
      this.localizedLoadPath = localLoadPath;
      this.refcount = 0;
      this.mtime = -1;
      this.localizedBaseDir = baseDir;
      this.size = 0;
      this.subDir = subDir;
      this.uniqueString = uniqueString;
    }
    
    Path getBaseDir(){
      return this.localizedBaseDir;
    }
    
    // mark it as initialized
    void initComplete() {
      inited = true;
    }

    // is it initialized?
    boolean isInited() {
      return inited;
    }
    
    Path getLocalizedUniqueDir() {
      return new Path(localizedBaseDir, new Path(subDir, uniqueString));
    }
  }

  /**
   * Clear the entire contents of the cache and delete the backing files. This
   * should only be used when the server is reinitializing, because the users
   * are going to lose their files.
   */
  public void purgeCache() {
    synchronized (cachedArchives) {
      for (Map.Entry<String,CacheStatus> f: cachedArchives.entrySet()) {
        try {
          deleteLocalPath(asyncDiskService, localFs, f.getValue().localizedLoadPath);
        } catch (IOException ie) {
          LOG.debug("Error cleaning up cache", ie);
        }
      }
      cachedArchives.clear();
    }
  }

  public TaskDistributedCacheManager newTaskDistributedCacheManager(
      Configuration taskConf) throws IOException {
    return new TaskDistributedCacheManager(this, taskConf);
  }

  /**
   * Determines timestamps of files to be cached, and stores those
   * in the configuration.  This is intended to be used internally by JobClient
   * after all cache files have been added.
   * 
   * This is an internal method!
   * 
   * @param job Configuration of a job.
   * @throws IOException
   */
  public static void determineTimestamps(Configuration job) throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      StringBuffer archiveTimestamps = 
        new StringBuffer(String.valueOf(
            getTimestamp(job, tarchives[0])));
      for (int i = 1; i < tarchives.length; i++) {
        archiveTimestamps.append(",");
        archiveTimestamps.append(String.valueOf(
            getTimestamp(job, tarchives[i])));
      }
      setArchiveTimestamps(job, archiveTimestamps.toString());
    }
  
    URI[] tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      StringBuffer fileTimestamps = new StringBuffer(String.valueOf(
          getTimestamp(job, tfiles[0])));
      for (int i = 1; i < tfiles.length; i++) {
        fileTimestamps.append(",");
        fileTimestamps.append(String.valueOf(
            getTimestamp(job, tfiles[i])));
      }
      setFileTimestamps(job, fileTimestamps.toString());
    }
  }
  /**
   * Determines the visibilities of the distributed cache files and 
   * archives. The visibility of a cache path is "public" if the leaf component
   * has READ permissions for others, and the parent subdirs have 
   * EXECUTE permissions for others
   * @param job
   * @throws IOException
   */
  public static void determineCacheVisibilities(Configuration job) 
  throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      StringBuffer archiveVisibilities = 
        new StringBuffer(String.valueOf(isPublic(job, tarchives[0])));
      for (int i = 1; i < tarchives.length; i++) {
        archiveVisibilities.append(",");
        archiveVisibilities.append(String.valueOf(isPublic(job, tarchives[i])));
      }
      setArchiveVisibilities(job, archiveVisibilities.toString());
    }
    URI[] tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      StringBuffer fileVisibilities = 
        new StringBuffer(String.valueOf(isPublic(job, tfiles[0])));
      for (int i = 1; i < tfiles.length; i++) {
        fileVisibilities.append(",");
        fileVisibilities.append(String.valueOf(isPublic(job, tfiles[i])));
      }
      setFileVisibilities(job, fileVisibilities.toString());
    }
  }
  
  /**
   * Get the booleans on whether the files are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans 
   * @throws IOException
   */
  static String[] getFileVisibilities(Configuration conf) {
    return conf.getStrings(JobContext.CACHE_FILE_VISIBILITIES);
  }

  /**
   * Get the booleans on whether the archives are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans 
   */
  static String[] getArchiveVisibilities(Configuration conf) {
    return conf.getStrings(JobContext.CACHE_ARCHIVES_VISIBILITIES);
  }

  /**
   * This method checks if there is a conflict in the fragment names 
   * of the uris. Also makes sure that each uri has a fragment. It 
   * is only to be called if you want to create symlinks for 
   * the various archives and files.  May be used by user code.
   * @param uriFiles The uri array of urifiles
   * @param uriArchives the uri array of uri archives
   */
  public static boolean checkURIs(URI[]  uriFiles, URI[] uriArchives){
    if ((uriFiles == null) && (uriArchives == null)){
      return true;
    }
    if (uriFiles != null){
      for (int i = 0; i < uriFiles.length; i++){
        String frag1 = uriFiles[i].getFragment();
        if (frag1 == null)
          return false;
        for (int j=i+1; j < uriFiles.length; j++){
          String frag2 = uriFiles[j].getFragment();
          if (frag2 == null)
            return false;
          if (frag1.equalsIgnoreCase(frag2))
            return false;
        }
        if (uriArchives != null){
          for (int j = 0; j < uriArchives.length; j++){
            String frag2 = uriArchives[j].getFragment();
            if (frag2 == null){
              return false;
            }
            if (frag1.equalsIgnoreCase(frag2))
              return false;
            for (int k=j+1; k < uriArchives.length; k++){
              String frag3 = uriArchives[k].getFragment();
              if (frag3 == null)
                return false;
              if (frag2.equalsIgnoreCase(frag3))
                return false;
            }
          }
        }
      }
    }
    return true;
  }
  /**
   * This is to check the public/private visibility of the archives to be
   * localized.
   * 
   * @param conf Configuration which stores the timestamp's
   * @param booleans comma separated list of booleans (true - public)
   * The order should be the same as the order in which the archives are added.
   */
  static void setArchiveVisibilities(Configuration conf, String booleans) {
    conf.set(JobContext.CACHE_ARCHIVES_VISIBILITIES, booleans);
  }

  /**
   * This is to check the public/private visibility of the files to be localized
   * 
   * @param conf Configuration which stores the timestamp's
   * @param booleans comma separated list of booleans (true - public)
   * The order should be the same as the order in which the files are added.
   */
  static void setFileVisibilities(Configuration conf, String booleans) {
    conf.set(JobContext.CACHE_FILE_VISIBILITIES, booleans);
  }

  /**
   * This is to check the timestamp of the archives to be localized.
   * 
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of archives.
   * The order should be the same as the order in which the archives are added.
   */
  static void setArchiveTimestamps(Configuration conf, String timestamps) {
    conf.set(JobContext.CACHE_ARCHIVES_TIMESTAMPS, timestamps);
  }

  /**
   * This is to check the timestamp of the files to be localized.
   * 
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of files.
   * The order should be the same as the order in which the files are added.
   */
  static void setFileTimestamps(Configuration conf, String timestamps) {
    conf.set(JobContext.CACHE_FILE_TIMESTAMPS, timestamps);
  }
  
  /**
   * Set the conf to contain the location for localized archives.
   * 
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  static void setLocalArchives(Configuration conf, String str) {
    conf.set(JobContext.CACHE_LOCALARCHIVES, str);
  }

  /**
   * Set the conf to contain the location for localized files.
   * 
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  static void setLocalFiles(Configuration conf, String str) {
    conf.set(JobContext.CACHE_LOCALFILES, str);
  }
}
