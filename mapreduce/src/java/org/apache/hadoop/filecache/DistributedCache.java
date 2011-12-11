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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.TrackerDistributedCacheManager;

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
 * Optionally users can also direct it to symlink the distributed cache file(s)
 * into the working directory of the task.</p>
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
 *         localArchives = DistributedCache.getLocalCacheArchives(job);
 *         localFiles = DistributedCache.getLocalCacheFiles(job);
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
 * (e.g., {@link org.apache.hadoop.mapred.JobClient}).  For implementation
 * details, see {@link TrackerDistributedCacheManager} and 
 * {@link TaskDistributedCacheManager}.
 *
 * @see org.apache.hadoop.mapred.JobConf
 * @see org.apache.hadoop.mapred.JobClient
 * @see org.apache.hadoop.mapreduce.Job
 * @deprecated Use methods on {@link Job}.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DistributedCache extends
    org.apache.hadoop.mapreduce.filecache.DistributedCache {
  //
}
