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

package org.apache.hadoop.mapred;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.MapReduceBase;
import java.io.*;
import org.apache.hadoop.filecache.*;
import java.net.URI;
import java.net.URISyntaxException;

public class MRCaching {
  static String testStr = "This is a test file " + "used for testing caching "
      + "jars, zip and normal files.";

  /**
   * Using the wordcount example and adding caching to it. The cache
   * archives/files are set and then are checked in the map if they have been
   * localized or not.
   */
  public static class MapClass extends MapReduceBase implements Mapper {
    JobConf conf;

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();

    public void configure(JobConf jconf) {
      conf = jconf;
      try {
        Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
        FileSystem fs = FileSystem.get(conf);
        // read the cached files (unzipped, unjarred and text)
        // and put it into a single file /tmp/test.txt
        Path file = new Path("/tmp");
        if (!fs.mkdirs(file)) {
          throw new IOException("Mkdirs failed to create " + file.toString());
        }
        Path fileOut = new Path(file, "test.txt");
        fs.delete(fileOut);
        DataOutputStream out = fs.create(fileOut);

        for (int i = 0; i < localArchives.length; i++) {
          // read out the files from these archives
          File f = new File(localArchives[i].toString());
          File txt = new File(f, "test.txt");
          FileInputStream fin = new FileInputStream(txt);
          DataInputStream din = new DataInputStream(fin);
          String str = din.readLine();
          din.close();
          out.writeBytes(str);
          out.writeBytes("\n");
        }
        for (int i = 0; i < localFiles.length; i++) {
          // read out the files from these archives
          File txt = new File(localFiles[i].toString());
          FileInputStream fin = new FileInputStream(txt);
          DataInputStream din = new DataInputStream(fin);
          String str = din.readLine();
          out.writeBytes(str);
          out.writeBytes("\n");
        }
        out.close();
      } catch (IOException ie) {
        System.out.println(StringUtils.stringifyException(ie));
      }
    }

    public void map(WritableComparable key, Writable value,
        OutputCollector output, Reporter reporter) throws IOException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, one);
      }

    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class ReduceClass extends MapReduceBase implements Reducer {

    public void reduce(WritableComparable key, Iterator values,
        OutputCollector output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += ((IntWritable) values.next()).get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static boolean launchMRCache(String jobTracker, String indir,
      String outdir, String fileSys, JobConf conf, String input)
      throws IOException {
    final Path inDir = new Path(indir);
    final Path outDir = new Path(outdir);
    FileSystem fs = FileSystem.getNamed(fileSys, conf);
    fs.delete(outDir);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    conf.set("fs.default.name", fileSys);
    conf.set("mapred.job.tracker", jobTracker);
    conf.setJobName("cachetest");

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(MRCaching.MapClass.class);
    conf.setCombinerClass(MRCaching.ReduceClass.class);
    conf.setReducerClass(MRCaching.ReduceClass.class);
    conf.setInputPath(inDir);
    conf.setOutputPath(outDir);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setSpeculativeExecution(false);
    Path localPath = new Path("build/test/cache");
    Path txtPath = new Path(localPath, new Path("test.txt"));
    Path jarPath = new Path(localPath, new Path("test.jar"));
    Path zipPath = new Path(localPath, new Path("test.zip"));
    Path cacheTest = new Path("/tmp/cachedir");
    fs.delete(cacheTest);
    if (!fs.mkdirs(cacheTest)) {
      throw new IOException("Mkdirs failed to create " + cacheTest.toString());
    }
    fs.copyFromLocalFile(txtPath, cacheTest);
    fs.copyFromLocalFile(jarPath, cacheTest);
    fs.copyFromLocalFile(zipPath, cacheTest);
    // setting the cached archives to zip, jar and simple text files
    String archive1 = "dfs://" + fileSys + "/tmp/cachedir/test.jar";
    String archive2 = "dfs://" + fileSys + "/tmp/cachedir/test.zip"; 
    String file1 = "dfs://" + fileSys + "/tmp/cachedir/test.txt";
    URI uri1 = null;
    URI uri2 = null;
    URI uri3 = null;
    try{
      uri1 = new URI(archive1);
      uri2 = new URI(archive2);
      uri3 = new URI(file1);
    } catch(URISyntaxException ur){
    }
    DistributedCache.addCacheArchive(uri1, conf);
    DistributedCache.addCacheArchive(uri2, conf);
    DistributedCache.addCacheFile(uri3, conf);
    JobClient.runJob(conf);
    int count = 0;
    // after the job ran check to see if the the input from the localized cache
    // match the real string. check if there are 3 instances or not.
    Path result = new Path("/tmp/test.txt");
    {
      BufferedReader file = new BufferedReader(new InputStreamReader(fs
          .open(result)));
      String line = file.readLine();
      while (line != null) {
        if (!testStr.equals(line))
          return false;
        count++;
        line = file.readLine();

      }
      file.close();
    }
    if (count != 3)
      return false;

    return true;

  }
}
