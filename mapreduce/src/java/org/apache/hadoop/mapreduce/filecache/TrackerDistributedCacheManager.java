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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskController.DistributedCacheFileContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
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
  private LinkedHashMap<String, CacheStatus> cachedArchives =
    new LinkedHashMap<String, CacheStatus>();

  // default total cache size (10GB)
  private static final long DEFAULT_CACHE_SIZE = 10737418240L;
  private static final long DEFAULT_CACHE_SUBDIR_LIMIT = 10000;
  // This default of 75 % was just pulled out of the air, it should be set
  // based off of read world use cases.
  private static final float DEFAULT_CACHE_KEEP_AROUND_PCT = 0.75f;
  private long allowedCacheSize;
  private long allowedCacheSubdirs;
  private long allowedCacheSizeCleanupGoal;
  private long allowedCacheSubdirsCleanupGoal;

  private static final Log LOG =
    LogFactory.getLog(TrackerDistributedCacheManager.class);

  private final LocalFileSystem localFs;
  
  private LocalDirAllocator lDirAllocator;
  
  private TaskController taskController;
  
  private Configuration trackerConf;
  
  private Random random = new Random();

  private MRAsyncDiskService asyncDiskService;

  protected BaseDirManager baseDirManager = new BaseDirManager();
  protected CleanupThread cleanupThread;

  public TrackerDistributedCacheManager(Configuration conf,
      TaskController taskController) throws IOException {
    this.localFs = FileSystem.getLocal(conf);
    this.trackerConf = conf;
    this.lDirAllocator = new LocalDirAllocator(TTConfig.LOCAL_DIR);
    this.taskController = taskController;
      // setting the cache size to a default of 10GB
    this.allowedCacheSize = conf.getLong(TTConfig.TT_LOCAL_CACHE_SIZE,
          DEFAULT_CACHE_SIZE);
      // setting the cache number of subdirectories limit to a default of 10000
    this.allowedCacheSubdirs = conf.getLong(
          TTConfig.TT_LOCAL_CACHE_SUBDIRS_LIMIT, DEFAULT_CACHE_SUBDIR_LIMIT);
    double cleanupPct = conf.getFloat(TTConfig.TT_LOCAL_CACHE_KEEP_AROUND_PCT,
        DEFAULT_CACHE_KEEP_AROUND_PCT);
    this.allowedCacheSizeCleanupGoal =
      (long)(this.allowedCacheSize * cleanupPct);
    this.allowedCacheSubdirsCleanupGoal =
      (long)(this.allowedCacheSubdirs * cleanupPct);
    this.cleanupThread = new CleanupThread(conf);
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
    String key;
    key = getKey(cache, conf, confFileStamp, getLocalizedCacheOwner(isPublic));
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
          cachePath, "")), localPath, new Path(subDir), uniqueString, key);
        cachedArchives.put(key, lcacheStatus);
      }

      //mark the cache for use. 
      lcacheStatus.incRefCount();
    }
    
    boolean initSuccessful = false;
    try {
      // do the localization, after releasing the global lock
      synchronized (lcacheStatus) {
        if (!lcacheStatus.isInited()) {
          FileSystem fs = FileSystem.get(cache, conf);
          checkStampSinceJobStarted(conf, fs, cache, confFileStamp,
              lcacheStatus, fileStatus);
          localizedPath = localizeCache(conf, cache, confFileStamp,
              lcacheStatus, isArchive, isPublic);
          lcacheStatus.initComplete();
        } else {
          localizedPath = checkCacheStatusValidity(conf, cache, confFileStamp,
              lcacheStatus, fileStatus, isArchive);
        }
        createSymlink(conf, cache, lcacheStatus, isArchive, currentWorkDir,
            honorSymLinkConf);
      }
      initSuccessful = true;
      return localizedPath;
    } finally {
      if (!initSuccessful) {
        lcacheStatus.decRefCount();
      }
    }
  }

  /**
   * This is the opposite of getlocalcache. When you are done with
   * using the cache, you need to release the cache
   * @param cache The cache URI to be released
   * @param conf configuration which contains the filesystem the cache
   * @param timeStamp the timestamp on the file represented by the cache URI
   * @param owner the owner of the localized file
   * is contained in.
   * @throws IOException
   */
  void releaseCache(URI cache, Configuration conf, long timeStamp,
      String owner) throws IOException {
    String key = getKey(cache, conf, timeStamp, owner);
    synchronized (cachedArchives) {
      CacheStatus lcacheStatus = cachedArchives.get(key);
      if (lcacheStatus == null) {
        LOG.warn("Cannot find localized cache: " + cache + 
                 " (key: " + key + ") in releaseCache!");
        return;
      }
      
      // decrement ref count 
      lcacheStatus.decRefCount();
    }
  }

  /*
   * This method is called from unit tests. 
   */
  int getReferenceCount(URI cache, Configuration conf, long timeStamp,
      String owner) throws IOException {
    String key = getKey(cache, conf, timeStamp, owner);
    synchronized (cachedArchives) {
      CacheStatus lcacheStatus = cachedArchives.get(key);
      if (lcacheStatus == null) {
        throw new IOException("Cannot find localized cache: " + cache);
      }
      return lcacheStatus.getRefCount();
    }
  }
  
  /**
   * Get the user who should "own" the localized distributed cache file.
   * If the cache is public, the tasktracker user is the owner. If private,
   * the user that the task is running as, is the owner.
   * @param isPublic
   * @return the owner as a shortname string
   * @throws IOException
   */
  static String getLocalizedCacheOwner(boolean isPublic) throws IOException {
    String user;
    if (isPublic) {
      user = UserGroupInformation.getLoginUser().getShortUserName();
    } else {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    }
    return user;
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

  String getKey(URI cache, Configuration conf, long timeStamp, String user)
      throws IOException {
    return makeRelative(cache, conf) + String.valueOf(timeStamp) + user;
  }
  
  /**
   * Returns {@link FileStatus} of a given cache file on hdfs.
   * 
   * @param conf configuration
   * @param cache cache file 
   * @return {@link FileStatus} of a given cache file on hdfs
   * @throws IOException
   */
  static FileStatus getFileStatus(Configuration conf, URI cache)
    throws IOException {
    return ClientDistributedCacheManager.getFileStatus(conf, cache);
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
    return getFileStatus(conf, cache).getModificationTime();
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
    // Increase the size and sub directory count of the cache
    // from baseDirSize and baseDirNumberSubDir.
    baseDirManager.addCacheUpdate(cacheStatus);

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
          conf.get(MRJobConfig.USER_NAME), new File(cacheStatus.localizedBaseDir
              .toString()), cacheStatus.localizedBaseDir,
          cacheStatus.uniqueString);
      taskController.initializeDistributedCacheFile(context);
    }
  }

  private static boolean isTarFile(String filename) {
    return (filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
           filename.endsWith(".tar"));
  }
  
  // ensure that the file on hdfs hasn't been modified since the job started
  private long checkStampSinceJobStarted(Configuration conf, FileSystem fs,
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

    if (dfsFileStamp != confFileStamp) {
      LOG.fatal("File: " + cache + " has changed on HDFS since job started");
      throw new IOException("File: " + cache +
                            " has changed on HDFS since job started");
    }
    
    return dfsFileStamp;
  }

  // Checks if the cache has already been localized and is fresh
  private boolean ifExistsAndFresh(Configuration conf, FileSystem fs,
                                          URI cache, long confFileStamp,
                                          CacheStatus lcacheStatus,
                                          FileStatus fileStatus)
  throws IOException {
    long dfsFileStamp = checkStampSinceJobStarted(conf, fs, cache,
        confFileStamp, lcacheStatus, fileStatus);
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

  class CacheStatus {
    //
    // This field should be accessed under global cachedArchives lock.
    //
    private int refcount;           // number of instances using this cache.

    //
    // The following three fields should be accessed under
    // individual cacheStatus lock.
    //
    long size;              //the size of this cache.
    long mtime;             // the cache-file modification time
    boolean inited = false; // is it initialized ?

    //
    // The following four fields are Immutable.
    //
    // The sub directory (tasktracker/archive or tasktracker/user/archive),
    // under which the file will be localized
    final Path subDir;
    // unique string used in the construction of local load path
    final String uniqueString;
    // the local load path of this cache
    final Path localizedLoadPath;
    //the base dir where the cache lies
    final Path localizedBaseDir;
    //The key of this in the cachedArchives.
    private final String key;

    public CacheStatus(Path baseDir, Path localLoadPath, Path subDir,
        String uniqueString, String key) {
      super();
      this.localizedLoadPath = localLoadPath;
      this.refcount = 0;
      this.mtime = -1;
      this.localizedBaseDir = baseDir;
      this.size = 0;
      this.subDir = subDir;
      this.uniqueString = uniqueString;
      this.key = key;
    }

    public synchronized void incRefCount() {
      refcount += 1;
    }

    public void decRefCount() {
      synchronized (cachedArchives) {
        synchronized (this) {
          refcount -= 1;
          if(refcount <= 0) {
            String key = this.key;
            cachedArchives.remove(key);
            cachedArchives.put(key, this);
          }
        }
      }
    }

    public int getRefCount() {
      return refcount;
    }

    public synchronized boolean isUsed() {
      return refcount > 0;
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
   * Get the booleans on whether the files are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans 
   * @throws IOException
   */
  static String[] getFileVisibilities(Configuration conf) {
    return conf.getStrings(MRJobConfig.CACHE_FILE_VISIBILITIES);
  }

  /**
   * Get the booleans on whether the archives are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans 
   */
  static String[] getArchiveVisibilities(Configuration conf) {
    return conf.getStrings(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES);
  }

  /**
   * Set the conf to contain the location for localized archives.
   * 
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  static void setLocalArchives(Configuration conf, String str) {
    conf.set(MRJobConfig.CACHE_LOCALARCHIVES, str);
  }

  /**
   * Set the conf to contain the location for localized files.
   * 
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  public static void setLocalFiles(Configuration conf, String str) {
    conf.set(MRJobConfig.CACHE_LOCALFILES, str);
  }
  
  /**
   * A thread to check and cleanup the unused files periodically
   */
  protected class CleanupThread extends Thread {
    // How often do we check if we need to clean up cache files?
    private long cleanUpCheckPeriod = 60000L; // 1 minute
    public CleanupThread(Configuration conf) {
      cleanUpCheckPeriod =
        conf.getLong(TTConfig.TT_DISTRIBUTED_CACHE_CHECK_PERIOD,
                            cleanUpCheckPeriod);
    }

    private volatile boolean running = true;
    public void stopRunning() {
      running = false;
    }
    @Override
    public void run() {
      while (running) {
        try {
          Thread.sleep(cleanUpCheckPeriod);
          baseDirManager.checkAndCleanup();
        } catch (IOException e) {
          LOG.error("Exception in DistributedCache CleanupThread.", e);
        } catch (InterruptedException e) {
          LOG.info("Cleanup...",e);
          //To force us to exit cleanly
          running = false;
        } catch (Throwable t) {
          exitTaskTracker(t);
        }
      }
    }

    /**
     * Exit the task tracker because of a fatal error.
     */
    protected void exitTaskTracker(Throwable t) {
      LOG.fatal("Distributed Cache cleanup thread received runtime exception." +
          " Exiting the TaskTracker", t);
      Runtime.getRuntime().exit(-1);
    }
  }

  private static class CacheDir {
    long size;
    long subdirs;
  }

  /**
   * This class holds properties of each base directories and is responsible
   * for clean up unused cache files in base directories.
   */
  protected class BaseDirManager {

    private TreeMap<Path, CacheDir> properties =
      new TreeMap<Path, CacheDir>();

    /**
     * Check each base directory to see if the size or number of subdirectories
     * are exceed the limit. If the limit is exceeded, start deleting caches
     * with zero reference count. This method synchronizes cachedArchives.
     */
    public void checkAndCleanup() throws IOException {
      Collection<CacheStatus> toBeDeletedCache = new LinkedList<CacheStatus>();
      HashMap<Path, CacheDir> toBeCleanedBaseDir =
        new HashMap<Path, CacheDir>();
      synchronized (properties) {
        for (Map.Entry<Path, CacheDir> baseDir : properties.entrySet()) {
          CacheDir baseDirCounts = baseDir.getValue();
          if (allowedCacheSize < baseDirCounts.size ||
              allowedCacheSubdirs < baseDirCounts.subdirs) {
            CacheDir tcc = new CacheDir();
            tcc.size = baseDirCounts.size - allowedCacheSizeCleanupGoal;
            tcc.subdirs = baseDirCounts.subdirs - allowedCacheSubdirsCleanupGoal;
            toBeCleanedBaseDir.put(baseDir.getKey(), tcc);
          }
        }
      }
      synchronized (cachedArchives) {
        for (Iterator<Map.Entry<String, CacheStatus>> it =
             cachedArchives.entrySet().iterator(); it.hasNext();) {
          Map.Entry<String, CacheStatus> entry = it.next();
          String cacheId = entry.getKey();
          CacheStatus cacheStatus = cachedArchives.get(cacheId);
          CacheDir leftToClean = toBeCleanedBaseDir.get(cacheStatus.getBaseDir());
          if (leftToClean != null
              && (leftToClean.size > 0 || leftToClean.subdirs > 0)) {
            // if reference count is zero mark the cache for deletion
            if (!cacheStatus.isUsed()) {
              leftToClean.size -= cacheStatus.size;
              leftToClean.subdirs--;
              // delete this cache entry from the global list 
              // and mark the localized file for deletion
              toBeDeletedCache.add(cacheStatus);
              it.remove();
            }
          }
        }
      }
      // do the deletion, after releasing the global lock
      for (CacheStatus cacheStatus : toBeDeletedCache) {
        synchronized (cacheStatus) {
          deleteLocalPath(asyncDiskService, FileSystem.getLocal(trackerConf),
                          cacheStatus.getLocalizedUniqueDir());
          // Update the maps baseDirSize and baseDirNumberSubDir
          deleteCacheUpdate(cacheStatus);
        }
      }
    }

    /**
     * Decrement the size and sub directory count of the cache from baseDirSize
     * and baseDirNumberSubDir. Have to synchronize cacheStatus before calling
     * this method
     * @param cacheStatus cache status of the cache is deleted
     */
    public void deleteCacheUpdate(CacheStatus cacheStatus) {
      if (!cacheStatus.inited) {
        // if it is not created yet, do nothing.
        return;
      }
      synchronized (properties) {
        CacheDir cacheDir = properties.get(cacheStatus.getBaseDir());
        if (cacheDir != null) {
          cacheDir.size -= cacheStatus.size;
          cacheDir.subdirs--;
        } else {
          LOG.warn("Cannot find size and number of subdirectories of" +
              " baseDir: " + cacheStatus.getBaseDir());
        }
      }
    }

    /**
     * Update the maps baseDirSize and baseDirNumberSubDir when adding cache.
     * Increase the size and sub directory count of the cache from baseDirSize
     * and baseDirNumberSubDir. Have to synchronize cacheStatus before calling
     * this method.
     * @param cacheStatus cache status of the cache is added
     */
    public void addCacheUpdate(CacheStatus cacheStatus) {
      long cacheSize = cacheStatus.size;
      LOG.info("Adding in cache "+cacheStatus.localizedLoadPath+" at "+cacheStatus.localizedBaseDir+" size:"+cacheStatus.size);
      synchronized (properties) {
        CacheDir cacheDir = properties.get(cacheStatus.getBaseDir());
        if (cacheDir != null) {
          cacheDir.size += cacheSize;
          cacheDir.subdirs++;
        } else {
          cacheDir = new CacheDir();
          cacheDir.size = cacheSize;
          cacheDir.subdirs = 1;
          properties.put(cacheStatus.getBaseDir(), cacheDir);
        }
      }
    }
  }

  /**
   * Start the background thread
   */
  public void startCleanupThread() {
    this.cleanupThread.start();
  }

  /**
   * Stop the background thread
   */
  public void stopCleanupThread() {
    cleanupThread.stopRunning();
    cleanupThread.interrupt();
  }
}
