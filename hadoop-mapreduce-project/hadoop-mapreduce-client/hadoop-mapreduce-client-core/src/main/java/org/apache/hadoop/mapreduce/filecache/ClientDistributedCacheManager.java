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

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;

/**
 * Manages internal configuration of the cache by the client for job submission.
 */
@InterfaceAudience.Private
public class ClientDistributedCacheManager {

  /**
   * Determines timestamps of files to be cached, and stores those
   * in the configuration. Determines the visibilities of the distributed cache
   * files and archives. The visibility of a cache path is "public" if the leaf
   * component has READ permissions for others, and the parent subdirs have 
   * EXECUTE permissions for others.
   * 
   * This is an internal method!
   * 
   * @param job
   * @throws IOException
   */
  public static void determineTimestampsAndCacheVisibilities(Configuration job)
  throws IOException {
    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    determineTimestamps(job, statCache);
    determineCacheVisibilities(job, statCache);
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
  public static void determineTimestamps(Configuration job,
      Map<URI, FileStatus> statCache) throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      FileStatus status = getFileStatus(job, tarchives[0], statCache);
      StringBuilder archiveFileSizes =
        new StringBuilder(String.valueOf(status.getLen()));
      StringBuilder archiveTimestamps =
        new StringBuilder(String.valueOf(status.getModificationTime()));
      for (int i = 1; i < tarchives.length; i++) {
        status = getFileStatus(job, tarchives[i], statCache);
        archiveFileSizes.append(",");
        archiveFileSizes.append(String.valueOf(status.getLen()));
        archiveTimestamps.append(",");
        archiveTimestamps.append(String.valueOf(status.getModificationTime()));
      }
      job.set(MRJobConfig.CACHE_ARCHIVES_SIZES, archiveFileSizes.toString());
      setArchiveTimestamps(job, archiveTimestamps.toString());
    }
  
    URI[] tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      FileStatus status = getFileStatus(job, tfiles[0], statCache);
      StringBuilder fileSizes =
        new StringBuilder(String.valueOf(status.getLen()));
      StringBuilder fileTimestamps = new StringBuilder(String.valueOf(
        status.getModificationTime()));
      for (int i = 1; i < tfiles.length; i++) {
        status = getFileStatus(job, tfiles[i], statCache);
        fileSizes.append(",");
        fileSizes.append(String.valueOf(status.getLen()));
        fileTimestamps.append(",");
        fileTimestamps.append(String.valueOf(status.getModificationTime()));
      }
      job.set(MRJobConfig.CACHE_FILES_SIZES, fileSizes.toString());
      setFileTimestamps(job, fileTimestamps.toString());
    }
  }
  
  /**
   * For each archive or cache file - get the corresponding delegation token
   * @param job
   * @param credentials
   * @throws IOException
   */
  public static void getDelegationTokens(Configuration job,
      Credentials credentials) throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    URI[] tfiles = DistributedCache.getCacheFiles(job);
    
    int size = (tarchives!=null? tarchives.length : 0) + (tfiles!=null ? tfiles.length :0);
    Path[] ps = new Path[size];
    
    int i = 0;
    if (tarchives != null) {
      for (i=0; i < tarchives.length; i++) {
        ps[i] = new Path(tarchives[i].toString());
      }
    }
    
    if (tfiles != null) {
      for(int j=0; j< tfiles.length; j++) {
        ps[i+j] = new Path(tfiles[j].toString());
      }
    }
    
    TokenCache.obtainTokensForNamenodes(credentials, ps, job);
  }
  
  /**
   * Determines the visibilities of the distributed cache files and 
   * archives. The visibility of a cache path is "public" if the leaf component
   * has READ permissions for others, and the parent subdirs have 
   * EXECUTE permissions for others
   * @param job
   * @throws IOException
   */
  public static void determineCacheVisibilities(Configuration job,
      Map<URI, FileStatus> statCache) throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      StringBuilder archiveVisibilities =
        new StringBuilder(String.valueOf(isPublic(job, tarchives[0], statCache)));
      for (int i = 1; i < tarchives.length; i++) {
        archiveVisibilities.append(",");
        archiveVisibilities.append(String.valueOf(isPublic(job, tarchives[i], statCache)));
      }
      setArchiveVisibilities(job, archiveVisibilities.toString());
    }
    URI[] tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      StringBuilder fileVisibilities =
        new StringBuilder(String.valueOf(isPublic(job, tfiles[0], statCache)));
      for (int i = 1; i < tfiles.length; i++) {
        fileVisibilities.append(",");
        fileVisibilities.append(String.valueOf(isPublic(job, tfiles[i], statCache)));
      }
      setFileVisibilities(job, fileVisibilities.toString());
    }
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
    conf.set(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES, booleans);
  }

  /**
   * This is to check the public/private visibility of the files to be localized
   * 
   * @param conf Configuration which stores the timestamp's
   * @param booleans comma separated list of booleans (true - public)
   * The order should be the same as the order in which the files are added.
   */
  static void setFileVisibilities(Configuration conf, String booleans) {
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, booleans);
  }

  /**
   * This is to check the timestamp of the archives to be localized.
   * 
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of archives.
   * The order should be the same as the order in which the archives are added.
   */
  static void setArchiveTimestamps(Configuration conf, String timestamps) {
    conf.set(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS, timestamps);
  }

  /**
   * This is to check the timestamp of the files to be localized.
   * 
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of files.
   * The order should be the same as the order in which the files are added.
   */
  static void setFileTimestamps(Configuration conf, String timestamps) {
    conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS, timestamps);
  }

  /**
   * Gets the file status for the given URI.  If the URI is in the cache,
   * returns it.  Otherwise, fetches it and adds it to the cache.
   */
  private static FileStatus getFileStatus(Configuration job, URI uri,
      Map<URI, FileStatus> statCache) throws IOException {
    FileSystem fileSystem = FileSystem.get(uri, job);
    return getFileStatus(fileSystem, uri, statCache);
  }

  /**
   * Returns a boolean to denote whether a cache file is visible to all(public)
   * or not
   * @param conf the configuration
   * @param uri the URI to test
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException thrown if a file system operation fails
   */
  static boolean isPublic(Configuration conf, URI uri,
      Map<URI, FileStatus> statCache) throws IOException {
    boolean isPublic = true;
    FileSystem fs = FileSystem.get(uri, conf);
    Path current = new Path(uri.getPath());
    current = fs.makeQualified(current);

    // If we're looking at a wildcarded path, we only need to check that the
    // ancestors allow execution.  Otherwise, look for read permissions in
    // addition to the ancestors' permissions.
    if (!current.getName().equals(DistributedCache.WILDCARD)) {
      isPublic = checkPermissionOfOther(fs, current, FsAction.READ, statCache);
    }

    return isPublic &&
        ancestorsHaveExecutePermissions(fs, current.getParent(), statCache);
  }

  /**
   * Returns true if all ancestors of the specified path have the 'execute'
   * permission set for all users (i.e. that other users can traverse
   * the directory heirarchy to the given path)
   */
  static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path,
      Map<URI, FileStatus> statCache) throws IOException {
    Path current = path;
    while (current != null) {
      //the subdirs in the path should have execute permissions for others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE, statCache)) {
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
      FsAction action, Map<URI, FileStatus> statCache) throws IOException {
    FileStatus status = getFileStatus(fs, path.toUri(), statCache);
    FsPermission perms = status.getPermission();
    FsAction otherAction = perms.getOtherAction();
    if (otherAction.implies(action)) {
      return true;
    }
    return false;
  }

  private static FileStatus getFileStatus(FileSystem fs, URI uri,
      Map<URI, FileStatus> statCache) throws IOException {
    Path path = new Path(uri);

    if (path.getName().equals(DistributedCache.WILDCARD)) {
      path = path.getParent();
      uri = path.toUri();
    }

    FileStatus stat = statCache.get(uri);

    if (stat == null) {
      stat = fs.getFileStatus(path);
      statCache.put(uri, stat);
    }

    return stat;
  }
}
