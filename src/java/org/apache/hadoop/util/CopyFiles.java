/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * A Map-reduce program to recursively copy directories between
 * diffferent file-systems.
 *
 * @author Milind Bhandarkar
 */
public class CopyFiles extends MapReduceBase implements Reducer {
  
  private static final String usage = "cp <srcurl> <desturl>";
  
  /**
   * Mappper class for Copying files.
   */
  
  public static class CopyFilesMapper extends MapReduceBase implements Mapper {
    
    private int sizeBuf = 4096;
    private FileSystem srcFileSys = null;
    private FileSystem destFileSys = null;
    private Path srcPath = null;
    private Path destPath = null;
    private byte[] buffer = null;
    
    private void copy(String src) throws IOException {
      // open source file
      Path srcFile = new Path(srcPath, src);
      FSDataInputStream in = srcFileSys.open(srcFile);
      
      // create directories to hold destination file and create destFile
      Path destFile = new Path(destPath, src);
      Path destParent = destFile.getParent();
      if (destParent != null) { destFileSys.mkdirs(destParent); }
      FSDataOutputStream out = destFileSys.create(destFile);
      
      // copy file
      while (true) {
        int nread = in.read(buffer);
        if (nread < 0) {
          break;
        }
        out.write(buffer, 0, nread);
      }
      
      in.close();
      out.close();
    }
    
    /** Mapper configuration.
     * Extracts source and destination file system, as well as
     * top-level paths on source and destination directories.
     * Gets the named file systems, to be used later in map.
     */
    public void configure(JobConf job) {
      String srcfs = job.get("copy.src.fs", "local");
      String destfs = job.get("copy.dest.fs", "local");
      srcPath = new Path(job.get("copy.src.path", "/"));
      destPath = new Path(job.get("copy.dest.path", "/"));
      try {
        srcFileSys = FileSystem.getNamed(srcfs, job);
        destFileSys = FileSystem.getNamed(destfs, job);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to get the named file system.", ex);
      }
      sizeBuf = job.getInt("copy.buf.size", 4096);
      buffer = new byte[sizeBuf];
    }
    
    /** Map method. Copies one file from source file system to destination.
     * @param key source file name
     * @param value not-used.
     * @param out not-used.
     */
    public void map(WritableComparable key,
        Writable val,
        OutputCollector out,
        Reporter reporter) throws IOException {
      String src = ((UTF8) key).toString();
      copy(src);
    }
    
    public void close() {
      // nothing
    }
  }
  
  public void reduce(WritableComparable key,
      Iterator values,
      OutputCollector output,
      Reporter reporter) throws IOException {
    // nothing
  }
  
  private static String getFileSysName(URI url) {
    String fsname = url.getScheme();
    if ("dfs".equals(fsname)) {
      String host = url.getHost();
      int port = url.getPort();
      return (port==(-1)) ? host : (host+":"+port);
    } else {
      return "local";
    }
  }

  /**
   * Make a path relative with respect to a root path.
   * absPath is always assumed to descend from root.
   * Otherwise returned path is null.
   */
  private static Path makeRelative(Path root, Path absPath) {
    if (!absPath.isAbsolute()) { return absPath; }
    String sRoot = root.toString();
    String sPath = absPath.toString();
    Enumeration rootTokens = new StringTokenizer(sRoot, "/");
    ArrayList rList = Collections.list(rootTokens);
    Enumeration pathTokens = new StringTokenizer(sPath, "/");
    ArrayList pList = Collections.list(pathTokens);
    Iterator rIter = rList.iterator();
    Iterator pIter = pList.iterator();
    while (rIter.hasNext()) {
      String rElem = (String) rIter.next();
      String pElem = (String) pIter.next();
      if (!rElem.equals(pElem)) { return null; }
    }
    StringBuffer sb = new StringBuffer();
    while (pIter.hasNext()) {
      String pElem = (String) pIter.next();
      sb.append(pElem);
      if (pIter.hasNext()) { sb.append("/"); }
    }
    return new Path(sb.toString());
  }
  /**
   * This is the main driver for recursively copying directories
   * across file systems. It takes at least two cmdline parameters. A source
   * URL and a destination URL. It then essentially does an "ls -lR" on the
   * source URL, and writes the output in aa round-robin manner to all the map
   * input files. The mapper actually copies the files allotted to it. And
   * the reduce is empty.
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println(usage);
      return;
    }
    
    Configuration conf = new Configuration();
    
    String srcPath = args[0];
    String destPath = args[1];
    
    URI srcurl = null;
    URI desturl = null;
    try {
      srcurl = new URI(srcPath);
      desturl = new URI(destPath);
    } catch (URISyntaxException ex) {
      throw new RuntimeException("URL syntax error.", ex);
    }
    
    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("copy-files");
    
    String srcFileSysName = getFileSysName(srcurl);
    String destFileSysName = getFileSysName(desturl);
    
    jobConf.set("copy.src.fs", srcFileSysName);
    jobConf.set("copy.dest.fs", destFileSysName);
    FileSystem srcfs;
   
    srcfs = FileSystem.getNamed(srcFileSysName, conf);
    FileSystem destfs = FileSystem.getNamed(destFileSysName, conf);
 
    srcPath = srcurl.getPath();
    if ("".equals(srcPath)) { srcPath = "/"; }
    destPath = desturl.getPath();
    if ("".equals(destPath)) { destPath = "/"; }
    
    boolean isFile = false;
    Path tmpPath = new Path(srcPath);
    Path rootPath = new Path(srcPath);
    if (srcfs.isFile(tmpPath)) {
      isFile = true;
      tmpPath = tmpPath.getParent();
      rootPath = rootPath.getParent();
      jobConf.set("copy.src.path", tmpPath.toString());
    } else {
      jobConf.set("copy.src.path", srcPath);
    }
    jobConf.set("copy.dest.path", destPath);
    
    if (!srcfs.exists(tmpPath)) {
      System.out.println(srcPath+" does not exist.");
      return;
    }
    
    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobConf.setSpeculativeExecution(false);
    jobConf.setInputKeyClass(UTF8.class);
    jobConf.setInputValueClass(UTF8.class);
    jobConf.setInputFormat(SequenceFileInputFormat.class);
        
    jobConf.setOutputKeyClass(UTF8.class);
    jobConf.setOutputValueClass(UTF8.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    
    jobConf.setMapperClass(CopyFilesMapper.class);
    jobConf.setReducerClass(CopyFiles.class);
    
    int filesPerMap = jobConf.getInt("copy.files_per_map", 10);
    jobConf.setNumReduceTasks(1);

    Path tmpDir = new Path("copy-files");
    Path inDir = new Path(tmpDir, "in");
    Path fakeOutDir = new Path(tmpDir, "out");
    FileSystem fileSys = FileSystem.get(jobConf);
    fileSys.delete(tmpDir);
    fileSys.mkdirs(inDir);
    
    
    jobConf.setInputPath(inDir);
    jobConf.setOutputPath(fakeOutDir);
    
    // create new sequence-files for holding paths
    ArrayList pathList = new ArrayList();
    ArrayList finalPathList = new ArrayList();
    pathList.add(new Path(srcPath));
    int part = 0;
    while(!pathList.isEmpty()) {
      Path top = (Path) pathList.remove(0);
      if (srcfs.isFile(top)) {
        top = makeRelative(rootPath, top);
        finalPathList.add(top.toString());
      } else {
        Path[] paths = srcfs.listPaths(top);
        for (int idx = 0; idx < paths.length; idx++) {
          pathList.add(paths[idx]);
        }
      }
    }
    int numMaps = finalPathList.size() / filesPerMap;
    if (numMaps == 0) { numMaps = 1; }
    jobConf.setNumMapTasks(numMaps);
    SequenceFile.Writer[] writers = new SequenceFile.Writer[numMaps];
    
    for(int idx=0; idx < numMaps; ++idx) {
      Path file = new Path(inDir, "part"+idx);
      writers[idx] = new SequenceFile.Writer(fileSys, file, UTF8.class, UTF8.class);
    }
    while (!finalPathList.isEmpty()) {
      String top = (String) finalPathList.remove(0);
      UTF8 key = new UTF8(top);
      UTF8 value = new UTF8("");
      writers[part].append(key, value);
      part = (part+1)%numMaps;
    }

    for(part = 0; part < numMaps; part++) {
      writers[part].close();
      writers[part] = null;
    }
    
    try {
      JobClient.runJob(jobConf);
    } finally {
      fileSys.delete(tmpDir);
    }
  
  }
}
