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

import java.io.*;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.net.URI;

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
 * <p>The framework will copy the necessary files on to the worker node before
 * any tasks for the job are executed on that node. Its efficiency stems from
 * the fact that the files are only copied once per job and the ability to
 * cache archives which are un-archived on the workers.</p>
 *
 * <p><code>DistributedCache</code> can be used to distribute simple, read-only
 * data/text files and/or more complex types such as archives, jars etc.
 * Archives (zip, tar and tgz/tar.gz files) are un-archived at the worker nodes.
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
 *     DistributedCache.addCacheArchive(new URI("/myapp/map.zip"), job);
 *     DistributedCache.addFileToClassPath(new Path("/myapp/mylib.jar"), job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytar.tar"), job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytgz.tgz"), job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytargz.tar.gz"), job);
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
 * </pre></blockquote>
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
 * @see org.apache.hadoop.mapreduce.Job
 * @see org.apache.hadoop.mapred.JobConf
 * @see org.apache.hadoop.mapred.JobClient
 */
@Deprecated
@InterfaceAudience.Private
public class DistributedCache {
  public static final String WILDCARD = "*";
  
  /**
   * Set the configuration with the given set of archives.  Intended
   * to be used by user code.
   * @param archives The list of archives that need to be localized
   * @param conf Configuration which will be changed
   * @deprecated Use {@link Job#setCacheArchives(URI[])} instead
   * @see Job#setCacheArchives(URI[])
   */
  @Deprecated
  public static void setCacheArchives(URI[] archives, Configuration conf) {
    String sarchives = StringUtils.uriToString(archives);
    conf.set(MRJobConfig.CACHE_ARCHIVES, sarchives);
  }

  /**
   * Set the configuration with the given set of files.  Intended to be
   * used by user code.
   * @param files The list of files that need to be localized
   * @param conf Configuration which will be changed
   * @deprecated Use {@link Job#setCacheFiles(URI[])} instead
   * @see Job#setCacheFiles(URI[])
   */
  @Deprecated
  public static void setCacheFiles(URI[] files, Configuration conf) {
    String sfiles = StringUtils.uriToString(files);
    conf.set(MRJobConfig.CACHE_FILES, sfiles);
  }

  /**
   * Get cache archives set in the Configuration.  Used by
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which contains the archives
   * @return A URI array of the caches set in the Configuration
   * @throws IOException
   * @deprecated Use {@link JobContext#getCacheArchives()} instead
   * @see JobContext#getCacheArchives()
   */
  @Deprecated
  public static URI[] getCacheArchives(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings(MRJobConfig.CACHE_ARCHIVES));
  }

  /**
   * Get cache files set in the Configuration.  Used by internal
   * DistributedCache and MapReduce code.
   * @param conf The configuration which contains the files
   * @return A URI array of the files set in the Configuration
   * @throws IOException
   * @deprecated Use {@link JobContext#getCacheFiles()} instead
   * @see JobContext#getCacheFiles()
   */
  @Deprecated
  public static URI[] getCacheFiles(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings(MRJobConfig.CACHE_FILES));
  }

  /**
   * Return the path array of the localized caches.  Intended to be used
   * by user code.
   * @param conf Configuration that contains the localized archives
   * @return A path array of localized caches
   * @throws IOException
   * @deprecated Use {@link JobContext#getLocalCacheArchives()} instead
   * @see JobContext#getLocalCacheArchives()
   */
  @Deprecated
  public static Path[] getLocalCacheArchives(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf
                                    .getStrings(MRJobConfig.CACHE_LOCALARCHIVES));
  }

  /**
   * Return the path array of the localized files.  Intended to be used
   * by user code.
   * @param conf Configuration that contains the localized files
   * @return A path array of localized files
   * @throws IOException
   * @deprecated Use {@link JobContext#getLocalCacheFiles()} instead
   * @see JobContext#getLocalCacheFiles()
   */
  @Deprecated
  public static Path[] getLocalCacheFiles(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf.getStrings(MRJobConfig.CACHE_LOCALFILES));
  }

  /**
   * Parse a list of strings into longs.
   * @param strs the list of strings to parse
   * @return a list of longs that were parsed. same length as strs.
   */
  private static long[] parseTimestamps(String[] strs) {
    if (strs == null) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }

  /**
   * Get the timestamps of the archives.  Used by internal
   * DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a long array of timestamps
   * @deprecated Use {@link JobContext#getArchiveTimestamps()} instead
   * @see JobContext#getArchiveTimestamps()
   */
  @Deprecated
  public static long[] getArchiveTimestamps(Configuration conf) {
    return parseTimestamps(
        conf.getStrings(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS));
  }


  /**
   * Get the timestamps of the files.  Used by internal
   * DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a long array of timestamps
   * @deprecated Use {@link JobContext#getFileTimestamps()} instead
   * @see JobContext#getFileTimestamps()
   */
  @Deprecated
  public static long[] getFileTimestamps(Configuration conf) {
    return parseTimestamps(
        conf.getStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS));
  }

  /**
   * Add a archives to be localized to the conf.  Intended to
   * be used by user code.
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   * @deprecated Use {@link Job#addCacheArchive(URI)} instead
   * @see Job#addCacheArchive(URI)
   */
  @Deprecated
  public static void addCacheArchive(URI uri, Configuration conf) {
    String archives = conf.get(MRJobConfig.CACHE_ARCHIVES);
    conf.set(MRJobConfig.CACHE_ARCHIVES, archives == null ? uri.toString()
             : archives + "," + uri.toString());
  }

  /**
   * Add a file to be localized to the conf.  The localized file will be
   * downloaded to the execution node(s), and a link will created to the
   * file from the job's working directory. If the last part of URI's path name
   * is "*", then the entire parent directory will be localized and links
   * will be created from the job's working directory to each file in the
   * parent directory.
   *
   * The access permissions of the file will determine whether the localized
   * file will be shared across jobs.  If the file is not readable by other or
   * if any of its parent directories is not executable by other, then the
   * file will not be shared.  In the case of a path that ends in "/*",
   * sharing of the localized files will be determined solely from the
   * access permissions of the parent directories.  The access permissions of
   * the individual files will be ignored.
   *
   * Intended to be used by user code.
   *
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   * @deprecated Use {@link Job#addCacheFile(URI)} instead
   * @see Job#addCacheFile(URI)
   */
  @Deprecated
  public static void addCacheFile(URI uri, Configuration conf) {
    String files = conf.get(MRJobConfig.CACHE_FILES);
    conf.set(MRJobConfig.CACHE_FILES, files == null ? uri.toString() : files + ","
             + uri.toString());
  }

  /**
   * Add a file path to the current set of classpath entries.  The file will
   * also be added to the cache.  Intended to be used by user code.
   *
   * @param file Path of the file to be added
   * @param conf Configuration that contains the classpath setting
   * @deprecated Use {@link Job#addFileToClassPath(Path)} instead
   * @see #addCacheFile(URI, Configuration)
   * @see Job#addFileToClassPath(Path)
   */
  @Deprecated
  public static void addFileToClassPath(Path file, Configuration conf)
    throws IOException {
	  addFileToClassPath(file, conf, file.getFileSystem(conf));
  }

  /**
   * Add a file path to the current set of classpath entries. The file will
   * also be added to the cache.  Intended to be used by user code.
   *
   * @param file Path of the file to be added
   * @param conf Configuration that contains the classpath setting
   * @param fs FileSystem with respect to which {@code archivefile} should
   *              be interpreted.
   * @see #addCacheFile(URI, Configuration)
   */
  public static void addFileToClassPath(Path file, Configuration conf,
      FileSystem fs) {
    addFileToClassPath(file, conf, fs, true);
  }

  /**
   * Add a file path to the current set of classpath entries. The file will
   * also be added to the cache if {@code addToCache} is true.  Used by
   * internal DistributedCache code.
   *
   * @param file Path of the file to be added
   * @param conf Configuration that contains the classpath setting
   * @param fs FileSystem with respect to which {@code archivefile} should
   *              be interpreted.
   * @param addToCache whether the file should also be added to the cache list
   * @see #addCacheFile(URI, Configuration)
   */
  public static void addFileToClassPath(Path file, Configuration conf,
      FileSystem fs, boolean addToCache) {
    String classpath = conf.get(MRJobConfig.CLASSPATH_FILES);
    conf.set(MRJobConfig.CLASSPATH_FILES, classpath == null ? file.toString()
             : classpath + "," + file.toString());

    if (addToCache) {
      URI uri = fs.makeQualified(file).toUri();
      addCacheFile(uri, conf);
    }
  }

  /**
   * Get the file entries in classpath as an array of Path.
   * Used by internal DistributedCache code.
   *
   * @param conf Configuration that contains the classpath setting
   * @deprecated Use {@link JobContext#getFileClassPaths()} instead
   * @see JobContext#getFileClassPaths()
   */
  @Deprecated
  public static Path[] getFileClassPaths(Configuration conf) {
    ArrayList<String> list = (ArrayList<String>)conf.getStringCollection(
                                MRJobConfig.CLASSPATH_FILES);
    if (list.size() == 0) {
      return null;
    }
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path(list.get(i));
    }
    return paths;
  }

  /**
   * Add an archive path to the current set of classpath entries. It adds the
   * archive to cache as well.  Intended to be used by user code.
   *
   * @param archive Path of the archive to be added
   * @param conf Configuration that contains the classpath setting
   * @deprecated Use {@link Job#addArchiveToClassPath(Path)} instead
   * @see Job#addArchiveToClassPath(Path)
   */
  @Deprecated
  public static void addArchiveToClassPath(Path archive, Configuration conf)
    throws IOException {
    addArchiveToClassPath(archive, conf, archive.getFileSystem(conf));
  }

  /**
   * Add an archive path to the current set of classpath entries. It adds the
   * archive to cache as well.  Intended to be used by user code.
   *
   * @param archive Path of the archive to be added
   * @param conf Configuration that contains the classpath setting
   * @param fs FileSystem with respect to which {@code archive} should be interpreted.
   */
  public static void addArchiveToClassPath
         (Path archive, Configuration conf, FileSystem fs)
      throws IOException {
    String classpath = conf.get(MRJobConfig.CLASSPATH_ARCHIVES);
    conf.set(MRJobConfig.CLASSPATH_ARCHIVES, classpath == null ? archive
             .toString() : classpath + "," + archive.toString());
    URI uri = fs.makeQualified(archive).toUri();

    addCacheArchive(uri, conf);
  }

  /**
   * Get the archive entries in classpath as an array of Path.
   * Used by internal DistributedCache code.
   *
   * @param conf Configuration that contains the classpath setting
   * @deprecated Use {@link JobContext#getArchiveClassPaths()} instead 
   * @see JobContext#getArchiveClassPaths()
   */
  @Deprecated
  public static Path[] getArchiveClassPaths(Configuration conf) {
    ArrayList<String> list = (ArrayList<String>)conf.getStringCollection(
                                MRJobConfig.CLASSPATH_ARCHIVES);
    if (list.size() == 0) {
      return null;
    }
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path(list.get(i));
    }
    return paths;
  }

  /**
   * Originally intended to enable symlinks, but currently symlinks cannot be
   * disabled. This is a NO-OP.
   * @param conf the jobconf
   * @deprecated This is a NO-OP.
   */
  @Deprecated
  public static void createSymlink(Configuration conf){
    //NOOP
  }

  /**
   * Originally intended to check if symlinks should be used, but currently
   * symlinks cannot be disabled.
   * @param conf the jobconf
   * @return true
   * @deprecated symlinks are always created.
   */
  @Deprecated
  public static boolean getSymlink(Configuration conf){
    return true;
  }

  private static boolean[] parseBooleans(String[] strs) {
    if (null == strs) {
      return null;
    }
    boolean[] result = new boolean[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Boolean.parseBoolean(strs[i]);
    }
    return result;
  }

  /**
   * Get the booleans on whether the files are public or not.  Used by
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans
   */
  public static boolean[] getFileVisibilities(Configuration conf) {
    return parseBooleans(conf.getStrings(MRJobConfig.CACHE_FILE_VISIBILITIES));
  }

  /**
   * Get the booleans on whether the archives are public or not.  Used by
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans
   */
  public static boolean[] getArchiveVisibilities(Configuration conf) {
    return parseBooleans(conf.getStrings(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES));
  }

  /**
   * This method checks if there is a conflict in the fragment names
   * of the uris. Also makes sure that each uri has a fragment. It
   * is only to be called if you want to create symlinks for
   * the various archives and files.  May be used by user code.
   * @param uriFiles The uri array of urifiles
   * @param uriArchives the uri array of uri archives
   */
  public static boolean checkURIs(URI[] uriFiles, URI[] uriArchives) {
    if ((uriFiles == null) && (uriArchives == null)) {
      return true;
    }
    // check if fragment is null for any uri
    // also check if there are any conflicts in fragment names
    Set<String> fragments = new HashSet<String>();

    // iterate over file uris
    if (uriFiles != null) {
      for (int i = 0; i < uriFiles.length; i++) {
        String fragment = uriFiles[i].getFragment();
        if (fragment == null) {
          return false;
        }
        String lowerCaseFragment = StringUtils.toLowerCase(fragment);
        if (fragments.contains(lowerCaseFragment)) {
          return false;
        }
        fragments.add(lowerCaseFragment);
      }
    }

    // iterate over archive uris
    if (uriArchives != null) {
      for (int i = 0; i < uriArchives.length; i++) {
        String fragment = uriArchives[i].getFragment();
        if (fragment == null) {
          return false;
        }
        String lowerCaseFragment = StringUtils.toLowerCase(fragment);
        if (fragments.contains(lowerCaseFragment)) {
          return false;
        }
        fragments.add(lowerCaseFragment);
      }
    }
    return true;
  }

}
