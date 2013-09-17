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
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * Distribute application-specific large, read-only files efficiently.
 * 
 * <p><code>DistributedCache</code> is a facility provided by the Map-Reduce
 * framework to cache files (text, archives, jars etc.) needed by applications.
 * </p>
 * 
 * <p>Applications specify the files, via urls (hdfs:// or http://) to be cached 
 * via the {@link org.apache.hadoop.mapred.JobConf}. The
 * <code>DistributedCache</code> assumes that the files specified via urls are
 * already present on the {@link FileSystem} at the path specified by the url
 * and are accessible by every machine in the cluster.</p>
 * 
 * <p>The framework will copy the necessary files on to the slave node before 
 * any tasks for the job are executed on that node. Its efficiency stems from 
 * the fact that the files are only copied once per job and the ability to 
 * cache archives which are un-archived on the slaves.</p> 
 *
 * <p><code>DistributedCache</code> can be used to distribute simple, read-only
 * data/text files and/or more complex types such as archives, jars etc. 
 * Archives (zip, tar and tgz/tar.gz files) are un-archived at the slave nodes. 
 * Jars may be optionally added to the classpath of the tasks, a rudimentary 
 * software distribution mechanism.  Files have execution permissions.
 * In older version of Hadoop Map/Reduce users could optionally ask for symlinks
 * to be created in the working directory of the child task.  In the current 
 * version symlinks are always created.  If the URL does not have a fragment 
 * the name of the file or directory will be used. If multiple files or 
 * directories map to the same link name, the last one added, will be used.  All
 * others will not even be downloaded.</p>
 * 
 * <p><code>DistributedCache</code> tracks modification timestamps of the cache 
 * files. Clearly the cache files should not be modified by the application 
 * or externally while the job is executing.</p>
 * 
 * <p>Here is an illustrative example on how to use the 
 * <code>DistributedCache</code>:</p>
 * <p><blockquote><pre>
 *     // Setting up the cache for the application
 *     
 *     1. Copy the requisite files to the <code>FileSystem</code>:
 *     
 *     $ bin/hadoop fs -copyFromLocal lookup.dat /myapp/lookup.dat  
 *     $ bin/hadoop fs -copyFromLocal map.zip /myapp/map.zip  
 *     $ bin/hadoop fs -copyFromLocal mylib.jar /myapp/mylib.jar
 *     $ bin/hadoop fs -copyFromLocal mytar.tar /myapp/mytar.tar
 *     $ bin/hadoop fs -copyFromLocal mytgz.tgz /myapp/mytgz.tgz
 *     $ bin/hadoop fs -copyFromLocal mytargz.tar.gz /myapp/mytargz.tar.gz
 *     
 *     2. Setup the application's <code>JobConf</code>:
 *     
 *     JobConf job = new JobConf();
 *     DistributedCache.addCacheFile(new URI("/myapp/lookup.dat#lookup.dat"), 
 *                                   job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/map.zip", job);
 *     DistributedCache.addFileToClassPath(new Path("/myapp/mylib.jar"), job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytar.tar", job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytgz.tgz", job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytargz.tar.gz", job);
 *     
 *     3. Use the cached files in the {@link org.apache.hadoop.mapred.Mapper}
 *     or {@link org.apache.hadoop.mapred.Reducer}:
 *     
 *     public static class MapClass extends MapReduceBase  
 *     implements Mapper&lt;K, V, K, V&gt; {
 *     
 *       private Path[] localArchives;
 *       private Path[] localFiles;
 *       
 *       public void configure(JobConf job) {
 *         // Get the cached archives/files
 *         File f = new File("./map.zip/some/file/in/zip.txt");
 *       }
 *       
 *       public void map(K key, V value, 
 *                       OutputCollector&lt;K, V&gt; output, Reporter reporter) 
 *       throws IOException {
 *         // Use data from the cached archives/files here
 *         // ...
 *         // ...
 *         output.collect(k, v);
 *       }
 *     }
 *     
 * </pre></blockquote></p>
 *
 * It is also very common to use the DistributedCache by using
 * {@link org.apache.hadoop.util.GenericOptionsParser}.
 *
 * This class includes methods that should be used by users
 * (specifically those mentioned in the example above, as well
 * as {@link DistributedCache#addArchiveToClassPath(Path, Configuration)}),
 * as well as methods intended for use by the MapReduce framework
 * (e.g., {@link org.apache.hadoop.mapred.JobClient}).
 *
 * @see org.apache.hadoop.mapred.JobConf
 * @see org.apache.hadoop.mapred.JobClient
 * @see org.apache.hadoop.mapreduce.Job
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public class DistributedCache extends
    org.apache.hadoop.mapreduce.filecache.DistributedCache {
  /**
   * Warning: {@link #CACHE_FILES_SIZES} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_FILES_SIZES}
   */
  @Deprecated
  public static final String CACHE_FILES_SIZES =
      "mapred.cache.files.filesizes";

  /**
   * Warning: {@link #CACHE_ARCHIVES_SIZES} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_ARCHIVES_SIZES}
   */
  @Deprecated
  public static final String CACHE_ARCHIVES_SIZES =
    "mapred.cache.archives.filesizes";

  /**
   * Warning: {@link #CACHE_ARCHIVES_TIMESTAMPS} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_ARCHIVES_TIMESTAMPS}
   */
  @Deprecated
  public static final String CACHE_ARCHIVES_TIMESTAMPS =
      "mapred.cache.archives.timestamps";

  /**
   * Warning: {@link #CACHE_FILES_TIMESTAMPS} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_FILE_TIMESTAMPS}
   */
  @Deprecated
  public static final String CACHE_FILES_TIMESTAMPS =
      "mapred.cache.files.timestamps";

  /**
   * Warning: {@link #CACHE_ARCHIVES} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_ARCHIVES}
   */
  @Deprecated
  public static final String CACHE_ARCHIVES = "mapred.cache.archives";

  /**
   * Warning: {@link #CACHE_FILES} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_FILES}
   */
  @Deprecated
  public static final String CACHE_FILES = "mapred.cache.files";

  /**
   * Warning: {@link #CACHE_LOCALARCHIVES} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_LOCALARCHIVES}
   */
  @Deprecated
  public static final String CACHE_LOCALARCHIVES =
      "mapred.cache.localArchives";

  /**
   * Warning: {@link #CACHE_LOCALFILES} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_LOCALFILES}
   */
  @Deprecated
  public static final String CACHE_LOCALFILES = "mapred.cache.localFiles";

  /**
   * Warning: {@link #CACHE_SYMLINK} is not a *public* constant.
   * The variable is kept for M/R 1.x applications, M/R 2.x applications should
   * use {@link MRJobConfig#CACHE_SYMLINK}
   */
  @Deprecated
  public static final String CACHE_SYMLINK = "mapred.create.symlink";

  /**
   * Add a archive that has been localized to the conf.  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  @Deprecated
  public static void addLocalArchives(Configuration conf, String str) {
    String archives = conf.get(CACHE_LOCALARCHIVES);
    conf.set(CACHE_LOCALARCHIVES, archives == null ? str
        : archives + "," + str);
  }

  /**
   * Add a file that has been localized to the conf..  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  @Deprecated
  public static void addLocalFiles(Configuration conf, String str) {
    String files = conf.get(CACHE_LOCALFILES);
    conf.set(CACHE_LOCALFILES, files == null ? str
        : files + "," + str);
  }

  /**
   * This method create symlinks for all files in a given dir in another
   * directory. Currently symlinks cannot be disabled. This is a NO-OP.
   *
   * @param conf the configuration
   * @param jobCacheDir the target directory for creating symlinks
   * @param workDir the directory in which the symlinks are created
   * @throws IOException
   * @deprecated Internal to MapReduce framework.  Use DistributedCacheManager
   * instead.
   */
  @Deprecated
  public static void createAllSymlink(
      Configuration conf, File jobCacheDir, File workDir)
    throws IOException{
    // Do nothing
  }

  /**
   * Returns {@link FileStatus} of a given cache file on hdfs. Internal to
   * MapReduce.
   * @param conf configuration
   * @param cache cache file
   * @return <code>FileStatus</code> of a given cache file on hdfs
   * @throws IOException
   */
  @Deprecated
  public static FileStatus getFileStatus(Configuration conf, URI cache)
    throws IOException {
    FileSystem fileSystem = FileSystem.get(cache, conf);
    return fileSystem.getFileStatus(new Path(cache.getPath()));
  }

  /**
   * Returns mtime of a given cache file on hdfs. Internal to MapReduce.
   * @param conf configuration
   * @param cache cache file
   * @return mtime of a given cache file on hdfs
   * @throws IOException
   */
  @Deprecated
  public static long getTimestamp(Configuration conf, URI cache)
    throws IOException {
    return getFileStatus(conf, cache).getModificationTime();
  }

  /**
   * This is to check the timestamp of the archives to be localized.
   * Used by internal MapReduce code.
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of archives.
   * The order should be the same as the order in which the archives are added.
   */
  @Deprecated
  public static void setArchiveTimestamps(Configuration conf, String timestamps) {
    conf.set(CACHE_ARCHIVES_TIMESTAMPS, timestamps);
  }

  /**
   * This is to check the timestamp of the files to be localized.
   * Used by internal MapReduce code.
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of files.
   * The order should be the same as the order in which the files are added.
   */
  @Deprecated
  public static void setFileTimestamps(Configuration conf, String timestamps) {
    conf.set(CACHE_FILES_TIMESTAMPS, timestamps);
  }

  /**
   * Set the conf to contain the location for localized archives.  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  @Deprecated
  public static void setLocalArchives(Configuration conf, String str) {
    conf.set(CACHE_LOCALARCHIVES, str);
  }

  /**
   * Set the conf to contain the location for localized files.  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  @Deprecated
  public static void setLocalFiles(Configuration conf, String str) {
    conf.set(CACHE_LOCALFILES, str);
  }
}
