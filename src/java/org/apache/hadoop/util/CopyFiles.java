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

package org.apache.hadoop.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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
public class CopyFiles extends ToolBase {
  
  private static final String usage = "distcp "+
  "[-fs <namenode:port | local> ] [-jt <jobtracker:port | local>] " +
  "[-conf <config-file.xml>] " + "[-D <property=value>] "+
  "[-i] <srcurl> | -f <urilist_uri> <desturl>";
  
  private static final long MIN_BYTES_PER_MAP = 1L << 28;
  private static final int MAX_NUM_MAPS = 10000;
  private static final int MAX_MAPS_PER_NODE = 10;
  
  private static final String readFailuresAttribute = 
    "distcp.ignore.read.failures";
  
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }
  
  /**
   * Base-class for all mappers for distcp
   * @author Arun C Murthy
   */
  public static abstract class CopyFilesMapper extends MapReduceBase 
  {
    /**
     * Interface to initialize *distcp* specific map tasks.
     * @param conf : The dfs/mapred configuration.
     * @param jobConf : The handle to the jobConf object to be initialized.
     * @param srcPaths : The source paths.
     * @param destPath : The destination path.
     * @param ignoreReadFailures : Ignore read failures?
     * @throws IOException
     */
    public abstract void setup(Configuration conf, JobConf jobConf, 
        String[] srcPaths, String destPath, boolean ignoreReadFailures) 
    throws IOException;
    
    /**
     * Interface to cleanup *distcp* specific resources
     * @param conf : The dfs/mapred configuration.
     * @param jobConf : The handle to the jobConf object to be initialized.
     * @param srcPath : The source uri.
     * @param destPath : The destination uri.
     * @throws IOException
     */
    public abstract void cleanup(Configuration conf, JobConf jobConf, 
        String srcPath, String destPath) throws IOException;
    
    public static String getFileSysName(URI url) {
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
    public static Path makeRelative(Path root, Path absPath) {
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
    
  }
  
  /**
   * DFSCopyFilesMapper: The mapper for copying files from the DFS.
   * @author Milind Bhandarkar
   */
  public static class DFSCopyFilesMapper extends CopyFilesMapper 
  implements Mapper 
  {
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
    private boolean ignoreReadFailures;
    
    private void copy(String src, Reporter reporter) throws IOException {
      // open source file
      Path srcFile = new Path(srcPath, src);
      FSDataInputStream in = srcFileSys.open(srcFile);
      long totalBytes = srcFileSys.getLength(srcFile);
      
      // create directories to hold destination file and create destFile
      Path destFile = new Path(destPath, src);
      Path destParent = destFile.getParent();
      if (destParent != null) { 
        if (!destFileSys.mkdirs(destParent)) {
          throw new IOException("Mkdirs failed to create " + 
                                destParent.toString()); 
        }
      }
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
    
    /**
     * Initialize DFSCopyFileMapper specific job-configuration.
     * @param conf : The dfs/mapred configuration.
     * @param jobConf : The handle to the jobConf object to be initialized.
     * @param srcPaths : The source URIs.
     * @param destPath : The destination URI.
     * @param ignoreReadFailures : Ignore read failures?
     */
    public void setup(Configuration conf, JobConf jobConf, 
        String[] srcPaths, String destPath, 
        boolean ignoreReadFailures) 
    throws IOException
    {
      URI srcURI = null;
      URI destURI = null;
      try {
        srcURI = new URI(srcPaths[0]);
        destURI = new URI(destPath);
      } catch (URISyntaxException ex) {
        throw new RuntimeException("URL syntax error.", ex);
      }
      
      String srcFileSysName = getFileSysName(srcURI);
      String destFileSysName = getFileSysName(destURI);
      jobConf.set("copy.src.fs", srcFileSysName);
      jobConf.set("copy.dest.fs", destFileSysName);
      
      FileSystem srcfs = FileSystem.getNamed(srcFileSysName, conf);
      
      String srcPath = srcURI.getPath();
      if ("".equals(srcPath)) { srcPath = "/"; }
      destPath = destURI.getPath();
      if ("".equals(destPath)) { destPath = "/"; }
      
      Path tmpPath = new Path(srcPath);
      Path rootPath = new Path(srcPath);
      if (srcfs.isFile(tmpPath)) {
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
      jobConf.setInputFormat(SequenceFileInputFormat.class);
      
      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(Text.class);
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);
      
      jobConf.setMapperClass(DFSCopyFilesMapper.class);
      jobConf.setReducerClass(CopyFilesReducer.class);
      
      jobConf.setNumReduceTasks(1);
      jobConf.setBoolean(readFailuresAttribute, ignoreReadFailures);
      
      Random r = new Random();
      Path jobDirectory = new Path(jobConf.getSystemDir(), "distcp_" 
          + Integer.toString(Math.abs(r.nextInt()), 36));
      Path inDir = new Path(jobDirectory, "in");
      Path fakeOutDir = new Path(jobDirectory, "out");
      FileSystem fileSys = FileSystem.get(jobConf);
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " +
                              inDir.toString());
      }
      jobConf.set("distcp.job.dir", jobDirectory.toString());
      
      jobConf.setInputPath(inDir);
      jobConf.setOutputPath(fakeOutDir);
      
      // create new sequence-files for holding paths
      ArrayList pathList = new ArrayList();
      ArrayList finalPathList = new ArrayList();
      pathList.add(new Path(srcPath));
      long totalBytes = 0;
      //int part = 0;
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
      // is MAX_NUM_MAPS. It is also limited by MAX_MAPS_PER_NODE. Also for 
      // small files it is better to determine number of maps by amount of 
      // data per map.
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
        SequenceFile.Writer writer = 
          SequenceFile.createWriter(fileSys,conf,file,Text.class,Text.class);
        for (int ipath = idx; ipath < nFiles; ipath += numMaps) {
          String path = (String) finalPathList.get(ipath);
          writer.append(new Text(path), new Text(""));
        }
        writer.close();
      }
      finalPathList = null;
      
    }
    
    public void cleanup(Configuration conf, JobConf jobConf, 
        String srcPath, String destPath) 
    throws IOException
    {
      //Clean up jobDirectory
      Path jobDirectory = new Path(jobConf.get("distcp.job.dir", "/"));
      FileSystem fs = FileSystem.get(jobConf);
      
      if(!jobDirectory.equals("/")) {
        fs.delete(jobDirectory);
      }
    }
    
    /** Mapper configuration.
     * Extracts source and destination file system, as well as
     * top-level paths on source and destination directories.
     * Gets the named file systems, to be used later in map.
     */
    public void configure(JobConf job) 
    {
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
      ignoreReadFailures = job.getBoolean(readFailuresAttribute, false);
    }
    
    /** Map method. Copies one file from source file system to destination.
     * @param key source file name
     * @param value not-used.
     * @param out not-used.
     * @param reporter
     */
    public void map(WritableComparable key,
        Writable value,
        OutputCollector out,
        Reporter reporter) throws IOException {
      String src = ((Text) key).toString();
      try {
        copy(src, reporter);
      } catch (IOException except) {
        if (ignoreReadFailures) {
          reporter.setStatus("Failed to copy " + src + " : " + 
              StringUtils.stringifyException(except));
          try {
            destFileSys.delete(new Path(destPath, src));
          } catch (Throwable ex) {
            // ignore, we are just cleaning up
          }
        } else {
          throw except;
        }
      }
    }
    
    public void close() {
      // nothing
    }
    
  }
  
  public static class HTTPCopyFilesMapper extends CopyFilesMapper 
  implements Mapper 
  {
    private URI srcURI = null;
    private FileSystem destFileSys = null;
    private Path destPath = null;
    private JobConf jobConf = null;
    private boolean ignoreReadFailures;
    
    /**
     * Initialize HTTPCopyFileMapper specific job.
     * @param conf : The dfs/mapred configuration.
     * @param jobConf : The handle to the jobConf object to be initialized.
     * @param srcPaths : The source URI.
     * @param destPath : The destination URI.
     * @param ignoreReadFailures : Ignore read failures?
     */
    public void setup(Configuration conf, JobConf jobConf, 
        String[] srcPaths, String destPath, 
        boolean ignoreReadFailures) 
    throws IOException
    {
      //Destination
      URI destURI = null;
      try {
        destURI = new URI(destPath);
      } catch (URISyntaxException ue) {
        throw new IOException("Illegal destination path!");
      }
      String destFileSysName = getFileSysName(destURI);
      jobConf.set("copy.dest.fs", destFileSysName);
      destPath = destURI.getPath();
      jobConf.set("copy.dest.path", destPath);
      
      //Setup the MR-job configuration
      jobConf.setSpeculativeExecution(false);
      
      jobConf.setInputFormat(SequenceFileInputFormat.class);
      
      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(Text.class);
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);
      
      jobConf.setMapperClass(HTTPCopyFilesMapper.class);
      jobConf.setReducerClass(CopyFilesReducer.class);
      
      // ideal number of maps is one per file (if the map-launching overhead
      // were 0. It is limited by jobtrackers handling capacity, which lets say
      // is MAX_NUM_MAPS. It is also limited by MAX_MAPS_PER_NODE. Also for 
      // small files it is better to determine number of maps by 
      // amount of data per map.
      int nFiles = srcPaths.length;
      int numMaps = nFiles;
      if (numMaps > MAX_NUM_MAPS) { numMaps = MAX_NUM_MAPS; }
      JobClient client = new JobClient(jobConf);
      ClusterStatus cluster = client.getClusterStatus();
      int tmpMaps = cluster.getTaskTrackers() * MAX_MAPS_PER_NODE;
      if (numMaps > tmpMaps) { numMaps = tmpMaps; }
      if (numMaps == 0) { numMaps = 1; }
      jobConf.setNumMapTasks(numMaps);
      
      jobConf.setBoolean(readFailuresAttribute, ignoreReadFailures);
      
      FileSystem fileSystem = FileSystem.get(conf);
      Random r = new Random();
      Path jobDirectory = new Path(jobConf.getSystemDir(), "distcp_" + 
          Integer.toString(Math.abs(r.nextInt()), 36));
      Path jobInputDir = new Path(jobDirectory, "in");
      if (!fileSystem.mkdirs(jobInputDir)) {
        throw new IOException("Mkdirs failed to create " + jobInputDir.toString());
      }
      jobConf.setInputPath(jobInputDir);
      
      jobConf.set("distcp.job.dir", jobDirectory.toString());
      Path jobOutputDir = new Path(jobDirectory, "out");
      jobConf.setOutputPath(jobOutputDir);
      
      for(int i=0; i < srcPaths.length; ++i) {
        Path ipFile = new Path(jobInputDir, "part" + i);
        SequenceFile.Writer writer = 
          SequenceFile.createWriter(fileSystem, conf, ipFile,
                                    Text.class, Text.class);
        writer.append(new Text(srcPaths[i]), new Text(""));
        writer.close();
      }
    }	
    
    public void cleanup(Configuration conf, JobConf jobConf, 
        String srcPath, String destPath) 
    throws IOException
    {
      //Clean up jobDirectory
      Path jobDirectory = new Path(jobConf.get("distcp.job.dir", "/"));
      FileSystem fs = FileSystem.get(jobConf);
      
      if(!jobDirectory.equals("/")) {
        fs.delete(jobDirectory);
      }
    }
    
    public void configure(JobConf job)
    {
      //Save jobConf
      jobConf = job;
      
      try {
        //Destination
        destFileSys = 
          FileSystem.getNamed(job.get("copy.dest.fs", "local"), job);
        destPath = new Path(job.get("copy.dest.path", "/"));
        if(!destFileSys.exists(destPath)) {
          return;
        }
      } catch(IOException ioe) {
        return;
      }
      
      ignoreReadFailures = job.getBoolean(readFailuresAttribute, false);
    }
    
    public void map(WritableComparable key,
        Writable val,
        OutputCollector out,
        Reporter reporter) throws IOException 
        {
      //The url of the file
      try {
        srcURI = new URI(((Text)key).toString());
        
        //Construct the complete destination path
        File urlPath = new File(srcURI.getPath());
        Path destinationPath = new Path(destPath, urlPath.getName());
        
        //Copy the file 
        URL url = srcURI.toURL();
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        
        int bufferSize = jobConf.getInt("io.file.buffer.size", 4096);
        byte[] buffer = new byte[bufferSize];
        BufferedInputStream is = 
          new BufferedInputStream(connection.getInputStream());
        
        FSDataOutputStream os = 
          new FSDataOutputStream(destFileSys, destinationPath, true, 
              jobConf,	bufferSize, (short)jobConf.getInt("dfs.replication", 3), 
              jobConf.getLong("dfs.block.size", 67108864)
          );
        
        int readBytes = 0;
        while((readBytes = is.read(buffer, 0, bufferSize)) != -1) {
          os.write(buffer, 0, readBytes);
        }
        
        is.close();
        os.close();
        connection.disconnect();
        
        reporter.setStatus("Copied: " + srcURI.toString() + 
            " to: " + destinationPath.toString());
        
      } catch(Exception e) {
        reporter.setStatus("Failed to copy from: " + (Text)key);
        if(ignoreReadFailures) {
          return;
        } else {
          throw new IOException("Failed to copy from: " + (Text)key);
        }
      }
    }
  }
  
  /**
   * Factory to create requisite Mapper objects for distcp.
   * @author Arun C Murthy
   */
  private static class CopyMapperFactory
  {
    public static CopyFilesMapper getMapper(String protocol)
    {
      CopyFilesMapper mapper = null;
      
      if("dfs".equals(protocol) || "file".equals(protocol)) {
        mapper = new DFSCopyFilesMapper();
      } else if("http".equals(protocol)) {
        mapper = new HTTPCopyFilesMapper();
      }
      
      return mapper;
    }
  }
  
  public static class CopyFilesReducer extends MapReduceBase implements Reducer {
    public void reduce(WritableComparable key,
        Iterator values,
        OutputCollector output,
        Reporter reporter) throws IOException {
      // nothing
    }
  }
  
  private static String[] fetchSrcURIs(Configuration conf, URI srcListURI) throws IOException
  {
    ArrayList uris = new ArrayList();
    BufferedReader fis = null;
    
    String srcListURIScheme = srcListURI.getScheme();
    String srcListURIPath = srcListURI.getPath();
    
    if("file".equals(srcListURIScheme)) {
      fis = new BufferedReader(new FileReader(srcListURIPath));
    } else if("dfs".equals(srcListURIScheme)) {
      FileSystem fs = FileSystem.getNamed(CopyFilesMapper.getFileSysName(srcListURI), conf);
      fis = new BufferedReader(
          new InputStreamReader(new FSDataInputStream(fs, new Path(srcListURIPath), conf))
          );
    } else if("http".equals(srcListURIScheme)) {
      //Copy the file 
      URL url = srcListURI.toURL();
      HttpURLConnection connection = (HttpURLConnection)url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();
      
      fis = new BufferedReader(
          new InputStreamReader(connection.getInputStream())
          );
    } else {
      throw new IOException("Unsupported source list uri!");
    }

    String uri = null;
    while((uri = fis.readLine()) != null) {
      if(!uri.startsWith("#")) {
        uris.add(uri);
      }
    }
    fis.close();

    if(!uris.isEmpty()) {
      return (String[])uris.toArray(new String[0]);
    }
    
    return null;
  }
  
  /**
   * Helper function to parse input file and return source urls for 
   * a given protocol.
   * @param protocol : The protocol for which to find source urls.
   * @param inputFilePath : The file containing the urls.
   * @return
   */
  private static String[] parseInputFile(String protocol, String[] uris)
  throws IOException
  {
    ArrayList protocolURIs = new ArrayList();
    
    for(int i=0; i < uris.length; ++i) {
      if(uris[i].startsWith(protocol)) {
        protocolURIs.add(uris[i]);
      }
    }
    
    if(!protocolURIs.isEmpty()) {
      return (String[])protocolURIs.toArray(new String[0]);
    }
    
    return null;
  }
  
  /**
   * Driver to copy srcPath to destPath depending on required protocol.
   * @param conf : Configuration
   * @param srcPath : Source path
   * @param destPath : Destination path
   */
  public static void copy(Configuration conf, String srcPath, String destPath,
      boolean srcAsList, boolean ignoreReadFailures) 
  throws IOException
  {
    //Job configuration
    JobConf jobConf = new JobConf(conf, CopyFiles.class);
    jobConf.setJobName("distcp");
    
    //Sanity check for srcPath/destPath
    URI srcURI = null;
    try {
        srcURI = new URI(srcPath);
    } catch (URISyntaxException ex) {
      throw new IOException("Illegal source path!");
    }
    
    URI destURI = null;
    try {
      destURI = new URI(destPath);
    } catch (URISyntaxException ex) {
      throw new IOException("Illegal destination path!");
    }
  
    //Source paths
    String[] srcPaths = null;
    
    if(srcAsList) {
      srcPaths = fetchSrcURIs(conf, srcURI);
    }
    
    //Create the task-specific mapper 
    CopyFilesMapper mapper = null;
    if(srcAsList) {
      //Ugly?!
      
      // Protocol - 'dfs://'
      String[] dfsUrls = parseInputFile("dfs", srcPaths);
      if(dfsUrls != null) {
        for(int i=0; i < dfsUrls.length; ++i) {
          copy(conf, dfsUrls[i], destPath, false, ignoreReadFailures);
        }
      }
      
      // Protocol - 'file://'
      String[] localUrls = parseInputFile("file", srcPaths);
      if(localUrls != null) {
        for(int i=0; i < localUrls.length; ++i) {
          copy(conf, localUrls[i], destPath, false, ignoreReadFailures);
        }
      }
      
      // Protocol - 'http://'
      String[] httpUrls = parseInputFile("http", srcPaths);
      if(httpUrls != null) {
        srcPaths = httpUrls;
        mapper = CopyMapperFactory.getMapper("http");
      } else {
        //Done
        return;
      }
      
    } else {
      //Single source - ugly!
      String[] tmpSrcPath = {srcPath};
      srcPaths = tmpSrcPath;
      mapper = CopyMapperFactory.getMapper(srcURI.getScheme());
    }
    
    //Initialize the mapper
    mapper.setup(conf, jobConf, srcPaths, destPath, ignoreReadFailures);
    
    //We are good to go!
    try {
      JobClient.runJob(jobConf);
    } finally {
      mapper.cleanup(conf, jobConf, srcPath, destPath);
    }
    
  }
  
  /**
   * This is the main driver for recursively copying directories
   * across file systems. It takes at least two cmdline parameters. A source
   * URL and a destination URL. It then essentially does an "ls -lR" on the
   * source URL, and writes the output in aa round-robin manner to all the map
   * input files. The mapper actually copies the files allotted to it. And
   * the reduce is empty.
   */
  public int run(String[] args) throws Exception {
    String srcPath = null;
    String destPath = null;
    boolean ignoreReadFailures = false;
    boolean srcAsList = false;
    
    for (int idx = 0; idx < args.length; idx++) {
      if ("-i".equals(args[idx])) {
        ignoreReadFailures = true;
      } else if ("-f".equals(args[idx])) {
        srcAsList = true;
      } else if (srcPath == null) {
        srcPath = args[idx];
      } else if (destPath == null) {
        destPath = args[idx];
      } else {
        System.out.println(usage);
        return -1;
      }
    }
    
    // mandatory command-line parameters
    if (srcPath == null || destPath == null) {
      System.out.println(usage);
      return -1;
    }
    
    try {
      copy(conf, srcPath, destPath, srcAsList, ignoreReadFailures);
    } catch (Exception e) {
      System.out.println("Caught: " + e);
      return -1;
    }
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = new CopyFiles().doMain(
        new JobConf(new Configuration(), CopyFiles.class), 
        args);
    System.exit(res);
  }
  
}
