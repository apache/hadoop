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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
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
  
  private static final String usage = "distcp <srcurl> <desturl> "+
          "[-dfs <namenode:port | local> ] [-jt <jobtracker:port | local>] " +
          "[-config <config-file.xml>]";
  
  private static final long MIN_BYTES_PER_MAP = 1L << 28;
  private static final int MAX_NUM_MAPS = 10000;
  private static final int MAX_MAPS_PER_NODE = 10;
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
    private static final long reportInterval = 1L << 25;
    private long bytesSinceLastReport = 0L;
    private long totalBytesCopied = 0L;
    private static DecimalFormat percentFormat = new DecimalFormat("0.00");
    
    private void copy(String src, Reporter reporter) throws IOException {
      // open source file
      Path srcFile = new Path(srcPath, src);
      FSDataInputStream in = srcFileSys.open(srcFile);
      long totalBytes = srcFileSys.getLength(srcFile);
      
      // create directories to hold destination file and create destFile
      Path destFile = new Path(destPath, src);
      Path destParent = destFile.getParent();
      if (destParent != null) { destFileSys.mkdirs(destParent); }
      FSDataOutputStream out = destFileSys.create(destFile);
      
      // copy file
      while (true) {
        int nread = in.read(buffer);
        if (nread < 0) { break; }
        out.write(buffer, 0, nread);
        bytesSinceLastReport += nread;
        if (bytesSinceLastReport > reportInterval) {
            totalBytesCopied += bytesSinceLastReport;
            bytesSinceLastReport = 0L;
            reporter.setStatus("Copy "+ src + ": " + 
                               percentFormat.format(100.0 * totalBytesCopied / 
                                                    totalBytes) +
                               "% and " +
                               StringUtils.humanReadableInt(totalBytesCopied) +
                               " bytes");
        }
      }
      
      in.close();
      out.close();
      // report at least once for each file
      totalBytesCopied += bytesSinceLastReport;
      bytesSinceLastReport = 0L;
      reporter.setStatus("Finished. Bytes copied: " + 
                         StringUtils.humanReadableInt(totalBytesCopied));
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
      copy(src, reporter);
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

    Configuration conf = new Configuration();
    String srcPath = null;
    String destPath = null;
    
    for (int idx = 0; idx < args.length; idx++) {
        if ("-dfs".equals(args[idx])) {
            if (idx == (args.length-1)) {
                System.out.println(usage);
                return;
            }
            conf.set("fs.default.name", args[++idx]);
        } else if ("-jt".equals(args[idx])) {
            if (idx == (args.length-1)) {
                System.out.println(usage);
                return;
            }
            conf.set("mapred.job.tracker", args[++idx]);
        } else if ("-config".equals(args[idx])) {
            if (idx == (args.length-1)) {
                System.out.println(usage);
                return;
            }
            conf.addFinalResource(new Path(args[++idx]));
        } else {
            if (srcPath == null) {
                srcPath = args[idx];
            } else if (destPath == null) {
                destPath = args[idx];
            } else {
                System.out.println(usage);
                return;
            }
        }
    }
    
    // mandatory command-line parameters
    if (srcPath == null || destPath == null) {
        System.out.println(usage);
        return;
    }
    
    URI srcurl = null;
    URI desturl = null;
    try {
      srcurl = new URI(srcPath);
      desturl = new URI(destPath);
    } catch (URISyntaxException ex) {
      throw new RuntimeException("URL syntax error.", ex);
    }
    
    JobConf jobConf = new JobConf(conf, CopyFiles.class);
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
    long totalBytes = 0;
    int part = 0;
    while(!pathList.isEmpty()) {
      Path top = (Path) pathList.remove(0);
      if (srcfs.isFile(top)) {
        totalBytes += srcfs.getLength(top);
        top = makeRelative(rootPath, top);
        finalPathList.add(top.toString());
      } else {
        Path[] paths = srcfs.listPaths(top);
        for (int idx = 0; idx < paths.length; idx++) {
          pathList.add(paths[idx]);
        }
      }
    }
    // ideal number of maps is one per file (if the map-launching overhead
    // were 0. It is limited by jobtrackers handling capacity, which lets say
    // is MAX_NUM_MAPS. It is also limited by MAX_MAPS_PER_NODE. Also for small
    // files it is better to determine number of maps by amount of data per map.
    
    int nFiles = finalPathList.size();
    int numMaps = nFiles;
    if (numMaps > MAX_NUM_MAPS) { numMaps = MAX_NUM_MAPS; }
    if (numMaps > (int) (totalBytes / MIN_BYTES_PER_MAP)) {
        numMaps = (int) (totalBytes / MIN_BYTES_PER_MAP);
    }
    JobClient client = new JobClient(jobConf);
    ClusterStatus cluster = client.getClusterStatus();
    int tmpMaps = cluster.getTaskTrackers() * MAX_MAPS_PER_NODE;
    if (numMaps > tmpMaps) { numMaps = tmpMaps; }
    if (numMaps == 0) { numMaps = 1; }
    jobConf.setNumMapTasks(numMaps);
    
    for(int idx=0; idx < numMaps; ++idx) {
      Path file = new Path(inDir, "part"+idx);
      SequenceFile.Writer writer = new SequenceFile.Writer(fileSys, file, UTF8.class, UTF8.class);
      for (int ipath = idx; ipath < nFiles; ipath += numMaps) {
        String path = (String) finalPathList.get(ipath);
        writer.append(new UTF8(path), new UTF8(""));
      }
      writer.close();
    }
    finalPathList = null;
    
    try {
      JobClient.runJob(jobConf);
    } finally {
      fileSys.delete(tmpDir);
    }
  
  }
}
