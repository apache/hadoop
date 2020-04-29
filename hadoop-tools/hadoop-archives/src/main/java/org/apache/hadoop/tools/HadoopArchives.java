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

package org.apache.hadoop.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

/**
 * a archive creation utility.
 * This class provides methods that can be used 
 * to create hadoop archives. For understanding of 
 * Hadoop archives look at {@link HarFileSystem}.
 */
public class HadoopArchives implements Tool {
  public static final int VERSION = 3;
  private static final Logger LOG = LoggerFactory.getLogger(HadoopArchives.class);
  
  private static final String NAME = "har"; 
  private static final String ARCHIVE_NAME = "archiveName";
  private static final String REPLICATION = "r";
  private static final String PARENT_PATH = "p";
  private static final String HELP = "help";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String DST_DIR_LABEL = NAME + ".dest.path";
  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String SRC_COUNT_LABEL = NAME + ".src.count";
  static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
  static final String DST_HAR_LABEL = NAME + ".archive.name";
  static final String SRC_PARENT_LABEL = NAME + ".parent.path";
  /** the size of the blocks that will be created when archiving **/
  static final String HAR_BLOCKSIZE_LABEL = NAME + ".block.size";
  /** the replication factor for the file in archiving. **/
  static final String HAR_REPLICATION_LABEL = NAME + ".replication.factor";
  /** the size of the part files that will be created when archiving **/
  static final String HAR_PARTSIZE_LABEL = NAME + ".partfile.size";

  /** size of each part file size **/
  long partSize = 2 * 1024 * 1024 * 1024l;
  /** size of blocks in hadoop archives **/
  long blockSize = 512 * 1024 * 1024l;
  /** the desired replication degree; default is 3 **/
  short repl = 3;

  private static final String usage = "archive"
  + " <-archiveName <NAME>.har> <-p <parent path>> [-r <replication factor>]" +
      " <src>* <dest>" +
  "\n";
  
 
  private JobConf conf;

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf, HadoopArchives.class);
    }

    // This is for test purposes since MR2, different from Streaming
    // here it is not possible to add a JAR to the classpath the tool
    // will when running the mapreduce job.
    String testJar = System.getProperty(TEST_HADOOP_ARCHIVES_JAR_PATH, null);
    if (testJar != null) {
      this.conf.setJar(testJar);
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public HadoopArchives(Configuration conf) {
    setConf(conf);
  }

  // check the src paths
  private static void checkPaths(Configuration conf, List<Path> paths) throws
  IOException {
    for (Path p : paths) {
      FileSystem fs = p.getFileSystem(conf);
      fs.getFileStatus(p);
    }
  }

  /**
   * this assumes that there are two types of files file/dir
   * @param fs the input filesystem
   * @param fdir the filestatusdir of the path  
   * @param out the list of paths output of recursive ls
   * @throws IOException
   */
  private void recursivels(FileSystem fs, FileStatusDir fdir, List<FileStatusDir> out) 
  throws IOException {
    if (fdir.getFileStatus().isFile()) {
      out.add(fdir);
      return;
    }
    else {
      out.add(fdir);
      FileStatus[] listStatus = fs.listStatus(fdir.getFileStatus().getPath());
      fdir.setChildren(listStatus);
      for (FileStatus stat: listStatus) {
        FileStatusDir fstatDir = new FileStatusDir(stat, null);
        recursivels(fs, fstatDir, out);
      }
    }
  }

  /** HarEntry is used in the {@link HArchivesMapper} as the input value. */
  private static class HarEntry implements Writable {
    String path;
    String[] children;

    HarEntry() {}
    
    HarEntry(String path, String[] children) {
      this.path = path;
      this.children = children;
    }

    boolean isDir() {
      return children != null;      
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      path = Text.readString(in);

      if (in.readBoolean()) {
        children = new String[in.readInt()];
        for(int i = 0; i < children.length; i++) {
          children[i] = Text.readString(in);
        }
      } else {
        children = null;
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, path);

      final boolean dir = isDir();
      out.writeBoolean(dir);
      if (dir) {
        out.writeInt(children.length);
        for(String c : children) {
          Text.writeString(out, c);
        }
      }
    }
  }

  /**
   * Input format of a hadoop archive job responsible for 
   * generating splits of the file list
   */
  static class HArchiveInputFormat implements InputFormat<LongWritable, HarEntry> {

    //generate input splits from the src file lists
    public InputSplit[] getSplits(JobConf jconf, int numSplits)
    throws IOException {
      String srcfilelist = jconf.get(SRC_LIST_LABEL, "");
      if ("".equals(srcfilelist)) {
          throw new IOException("Unable to get the " +
              "src file for archive generation.");
      }
      long totalSize = jconf.getLong(TOTAL_SIZE_LABEL, -1);
      if (totalSize == -1) {
        throw new IOException("Invalid size of files to archive");
      }
      //we should be safe since this is set by our own code
      Path src = new Path(srcfilelist);
      FileSystem fs = src.getFileSystem(jconf);
      FileStatus fstatus = fs.getFileStatus(src);
      ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      LongWritable key = new LongWritable();
      final HarEntry value = new HarEntry();
      // the remaining bytes in the file split
      long remaining = fstatus.getLen();
      // the count of sizes calculated till now
      long currentCount = 0L;
      // the endposition of the split
      long lastPos = 0L;
      // the start position of the split
      long startPos = 0L;
      long targetSize = totalSize/numSplits;
      // create splits of size target size so that all the maps 
      // have equals sized data to read and write to.
      try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, src, jconf)) {
        while(reader.next(key, value)) {
          if (currentCount + key.get() > targetSize && currentCount != 0){
            long size = lastPos - startPos;
            splits.add(new FileSplit(src, startPos, size, (String[]) null));
            remaining = remaining - size;
            startPos = lastPos;
            currentCount = 0L;
          }
          currentCount += key.get();
          lastPos = reader.getPosition();
        }
        // the remaining not equal to the target size.
        if (remaining != 0) {
          splits.add(new FileSplit(src, startPos, remaining, (String[])null));
        }
      }
      return splits.toArray(new FileSplit[splits.size()]);
    }

    @Override
    public RecordReader<LongWritable, HarEntry> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<LongWritable, HarEntry>(job,
                 (FileSplit)split);
    }
  }

  private boolean checkValidName(String name) {
    Path tmp = new Path(name);
    if (tmp.depth() != 1) {
      return false;
    }
    if (name.endsWith(".har")) 
      return true;
    return false;
  }
  

  private Path largestDepth(List<Path> paths) {
    Path deepest = paths.get(0);
    for (Path p: paths) {
      if (p.depth() > deepest.depth()) {
        deepest = p;
      }
    }
    return deepest;
  }
  
  /**
   * truncate the prefix root from the full path
   * @param fullPath the full path
   * @param root the prefix root to be truncated
   * @return the relative path
   */
  private Path relPathToRoot(Path fullPath, Path root) {
    // just take some effort to do it 
    // rather than just using substring 
    // so that we do not break sometime later
    final Path justRoot = new Path(Path.SEPARATOR);
    if (fullPath.depth() == root.depth()) {
      return justRoot;
    }
    else if (fullPath.depth() > root.depth()) {
      Path retPath = new Path(fullPath.getName());
      Path parent = fullPath.getParent();
      for (int i=0; i < (fullPath.depth() - root.depth() -1); i++) {
        retPath = new Path(parent.getName(), retPath);
        parent = parent.getParent();
      }
      return new Path(justRoot, retPath);
    }
    return null;
  }

  /**
   * this method writes all the valid top level directories 
   * into the srcWriter for indexing. This method is a little
   * tricky. example- 
   * for an input with parent path /home/user/ and sources 
   * as /home/user/source/dir1, /home/user/source/dir2 - this 
   * will output <source, dir, dir1, dir2> (dir means that source is a dir
   * with dir1 and dir2 as children) and <source/dir1, file, null>
   * and <source/dir2, file, null>
   * @param srcWriter the sequence file writer to write the
   * directories to
   * @param paths the source paths provided by the user. They
   * are glob free and have full path (not relative paths)
   * @param parentPath the parent path that you want the archives
   * to be relative to. example - /home/user/dir1 can be archived with
   * parent as /home or /home/user.
   * @throws IOException
   */
  private void writeTopLevelDirs(SequenceFile.Writer srcWriter, 
      List<Path> paths, Path parentPath) throws IOException {
    // extract paths from absolute URI's
    List<Path> justPaths = new ArrayList<Path>();
    for (Path p: paths) {
      justPaths.add(new Path(p.toUri().getPath()));
    }
    /* find all the common parents of paths that are valid archive
     * paths. The below is done so that we do not add a common path
     * twice and also we need to only add valid child of a path that
     * are specified the user.
     */
    TreeMap<String, HashSet<String>> allpaths = new TreeMap<String, 
                                                HashSet<String>>();
    /* the largest depth of paths. the max number of times
     * we need to iterate
     */
    Path deepest = largestDepth(paths);
    Path root = new Path(Path.SEPARATOR);
    for (int i = parentPath.depth(); i < deepest.depth(); i++) {
      List<Path> parents = new ArrayList<Path>();
      for (Path p: justPaths) {
        if (p.compareTo(root) == 0){
          //do nothing
        }
        else {
          Path parent = p.getParent();
          if (null != parent) {
            if (allpaths.containsKey(parent.toString())) {
              HashSet<String> children = allpaths.get(parent.toString());
              children.add(p.getName());
            } 
            else {
              HashSet<String> children = new HashSet<String>();
              children.add(p.getName());
              allpaths.put(parent.toString(), children);
            }
            parents.add(parent);
          }
        }
      }
      justPaths = parents;
    }
    Set<Map.Entry<String, HashSet<String>>> keyVals = allpaths.entrySet();
    for (Map.Entry<String, HashSet<String>> entry : keyVals) {
      final Path relPath = relPathToRoot(new Path(entry.getKey()), parentPath);
      if (relPath != null) {
        final String[] children = new String[entry.getValue().size()];
        int i = 0;
        for(String child: entry.getValue()) {
          children[i++] = child;
        }
        append(srcWriter, 0L, relPath.toString(), children);
      }
    }
  }

  private void append(SequenceFile.Writer srcWriter, long len,
      String path, String[] children) throws IOException {
    srcWriter.append(new LongWritable(len), new HarEntry(path, children));
  }
    
  /**
   * A static class that keeps
   * track of status of a path 
   * and there children if path is a dir
   */
  static class FileStatusDir {
    private FileStatus fstatus;
    private FileStatus[] children = null;
    
    /**
     * constructor for filestatusdir
     * @param fstatus the filestatus object that maps to filestatusdir
     * @param children the children list if fs is a directory
     */
    FileStatusDir(FileStatus fstatus, FileStatus[] children) {
      this.fstatus  = fstatus;
      this.children = children;
    }
    
    /**
     * set children of this object
     * @param listStatus the list of children
     */
    public void setChildren(FileStatus[] listStatus) {
      this.children = listStatus;
    }

    /**
     * the filestatus of this object
     * @return the filestatus of this object
     */
    FileStatus getFileStatus() {
      return this.fstatus;
    }
    
    /**
     * the children list of this object, null if  
     * @return the children list
     */
    FileStatus[] getChildren() {
      return this.children;
    }
  }
  
  /**archive the given source paths into
   * the dest
   * @param parentPath the parent path of all the source paths
   * @param srcPaths the src paths to be archived
   * @param dest the dest dir that will contain the archive
   */
  void archive(Path parentPath, List<Path> srcPaths, 
      String archiveName, Path dest) throws IOException {
    checkPaths(conf, srcPaths);
    int numFiles = 0;
    long totalSize = 0;
    FileSystem fs = parentPath.getFileSystem(conf);
    this.blockSize = conf.getLong(HAR_BLOCKSIZE_LABEL, blockSize);
    this.partSize = conf.getLong(HAR_PARTSIZE_LABEL, partSize);
    conf.setLong(HAR_BLOCKSIZE_LABEL, blockSize);
    conf.setLong(HAR_PARTSIZE_LABEL, partSize);
    conf.set(DST_HAR_LABEL, archiveName);
    conf.set(SRC_PARENT_LABEL, fs.makeQualified(parentPath).toString());
    conf.setInt(HAR_REPLICATION_LABEL, repl);
    Path outputPath = new Path(dest, archiveName);
    FileOutputFormat.setOutputPath(conf, outputPath);
    FileSystem outFs = outputPath.getFileSystem(conf);
    if (outFs.exists(outputPath)) {
      throw new IOException("Archive path: "
          + outputPath.toString() + " already exists");
    }
    if (outFs.isFile(dest)) {
      throw new IOException("Destination " + dest.toString()
          + " should be a directory but is a file");
    }
    conf.set(DST_DIR_LABEL, outputPath.toString());
    Path stagingArea;
    try {
      stagingArea = JobSubmissionFiles.getStagingDir(new Cluster(conf), 
          conf);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    Path jobDirectory = new Path(stagingArea,
        NAME+"_"+Integer.toString(new Random().nextInt(Integer.MAX_VALUE), 36));
    FsPermission mapredSysPerms = 
      new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jobDirectory.getFileSystem(conf), jobDirectory,
                      mapredSysPerms);
    conf.set(JOB_DIR_LABEL, jobDirectory.toString());
    //get a tmp directory for input splits
    FileSystem jobfs = jobDirectory.getFileSystem(conf);
    Path srcFiles = new Path(jobDirectory, "_har_src_files");
    conf.set(SRC_LIST_LABEL, srcFiles.toString());
    SequenceFile.Writer srcWriter = SequenceFile.createWriter(jobfs, conf,
        srcFiles, LongWritable.class, HarEntry.class, 
        SequenceFile.CompressionType.NONE);
    // get the list of files 
    // create single list of files and dirs
    try {
      // write the top level dirs in first 
      writeTopLevelDirs(srcWriter, srcPaths, parentPath);
      srcWriter.sync();
      // these are the input paths passed 
      // from the command line
      // we do a recursive ls on these paths 
      // and then write them to the input file 
      // one at a time
      for (Path src: srcPaths) {
        ArrayList<FileStatusDir> allFiles = new ArrayList<FileStatusDir>();
        FileStatus fstatus = fs.getFileStatus(src);
        FileStatusDir fdir = new FileStatusDir(fstatus, null);
        recursivels(fs, fdir, allFiles);
        for (FileStatusDir statDir: allFiles) {
          FileStatus stat = statDir.getFileStatus();
          long len = stat.isDirectory()? 0:stat.getLen();
          final Path path = relPathToRoot(stat.getPath(), parentPath);
          final String[] children;
          if (stat.isDirectory()) {
            //get the children 
            FileStatus[] list = statDir.getChildren();
            children = new String[list.length];
            for (int i = 0; i < list.length; i++) {
              children[i] = list[i].getPath().getName();
            }
          }
          else {
            children = null;
          }
          append(srcWriter, len, path.toString(), children);
          srcWriter.sync();
          numFiles++;
          totalSize += len;
        }
      }
    } finally {
      srcWriter.close();
    }
    conf.setInt(SRC_COUNT_LABEL, numFiles);
    conf.setLong(TOTAL_SIZE_LABEL, totalSize);
    int numMaps = (int)(totalSize/partSize);
    //run atleast one map.
    conf.setNumMapTasks(numMaps == 0? 1:numMaps);
    conf.setNumReduceTasks(1);
    conf.setInputFormat(HArchiveInputFormat.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapperClass(HArchivesMapper.class);
    conf.setReducerClass(HArchivesReducer.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(conf, jobDirectory);
    //make sure no speculative execution is done
    conf.setSpeculativeExecution(false);
    JobClient.runJob(conf);
    //delete the tmp job directory
    try {
      jobfs.delete(jobDirectory, true);
    } catch(IOException ie) {
      LOG.info("Unable to clean tmp directory " + jobDirectory);
    }
  }

  static class HArchivesMapper 
  implements Mapper<LongWritable, HarEntry, IntWritable, Text> {
    private JobConf conf = null;
    int partId = -1 ; 
    Path tmpOutputDir = null;
    Path tmpOutput = null;
    String partname = null;
    Path rootPath = null;
    FSDataOutputStream partStream = null;
    FileSystem destFs = null;
    byte[] buffer;
    int buf_size = 128 * 1024;
    private int replication = 3;
    long blockSize = 512 * 1024 * 1024l;

    // configure the mapper and create 
    // the part file.
    // use map reduce framework to write into
    // tmp files. 
    public void configure(JobConf conf) {
      this.conf = conf;
      replication = conf.getInt(HAR_REPLICATION_LABEL, 3);
      // this is tightly tied to map reduce
      // since it does not expose an api 
      // to get the partition
      partId = conf.getInt(MRJobConfig.TASK_PARTITION, -1);
      // create a file name using the partition
      // we need to write to this directory
      tmpOutputDir = FileOutputFormat.getWorkOutputPath(conf);
      blockSize = conf.getLong(HAR_BLOCKSIZE_LABEL, blockSize);
      // get the output path and write to the tmp 
      // directory 
      partname = "part-" + partId;
      tmpOutput = new Path(tmpOutputDir, partname);
      rootPath = (conf.get(SRC_PARENT_LABEL, null) == null) ? null :
                  new Path(conf.get(SRC_PARENT_LABEL));
      if (rootPath == null) {
        throw new RuntimeException("Unable to read parent " +
        		"path for har from config");
      }
      try {
        destFs = tmpOutput.getFileSystem(conf);
        //this was a stale copy
        destFs.delete(tmpOutput, false);
        partStream = destFs.create(tmpOutput, false, conf.getInt("io.file.buffer.size", 4096), 
            destFs.getDefaultReplication(tmpOutput), blockSize);
      } catch(IOException ie) {
        throw new RuntimeException("Unable to open output file " + tmpOutput, ie);
      }
      buffer = new byte[buf_size];
    }

    // copy raw data.
    public void copyData(Path input, FSDataInputStream fsin, 
        FSDataOutputStream fout, Reporter reporter) throws IOException {
      try {
        for (int cbread=0; (cbread = fsin.read(buffer))>= 0;) {
          fout.write(buffer, 0,cbread);
          reporter.progress();
        }
      } finally {
        fsin.close();
      }
    }
    
    /**
     * get rid of / in the beginning of path
     * @param p the path
     * @return return path without /
     */
    private Path realPath(Path p, Path parent) {
      Path rootPath = new Path(Path.SEPARATOR);
      if (rootPath.compareTo(p) == 0) {
        return parent;
      }
      return new Path(parent, new Path(p.toString().substring(1)));
    }

    private static String encodeName(String s) 
      throws UnsupportedEncodingException {
      return URLEncoder.encode(s,"UTF-8");
    }

    private static String encodeProperties( FileStatus fStatus )
      throws UnsupportedEncodingException {
      String propStr = encodeName(
          fStatus.getModificationTime() + " "
        + fStatus.getPermission().toShort() + " "
        + encodeName(fStatus.getOwner()) + " "
        + encodeName(fStatus.getGroup()));
      return propStr;
    }

    // read files from the split input 
    // and write it onto the part files.
    // also output hash(name) and string 
    // for reducer to create index 
    // and masterindex files.
    public void map(LongWritable key, HarEntry value,
        OutputCollector<IntWritable, Text> out,
        Reporter reporter) throws IOException {
      Path relPath = new Path(value.path);
      int hash = HarFileSystem.getHarHash(relPath);
      String towrite = null;
      Path srcPath = realPath(relPath, rootPath);
      long startPos = partStream.getPos();
      FileSystem srcFs = srcPath.getFileSystem(conf);
      FileStatus srcStatus = srcFs.getFileStatus(srcPath);
      String propStr = encodeProperties(srcStatus);
      if (value.isDir()) { 
        towrite = encodeName(relPath.toString())
                  + " dir " + propStr + " 0 0 ";
        StringBuffer sbuff = new StringBuffer();
        sbuff.append(towrite);
        for (String child: value.children) {
          sbuff.append(encodeName(child) + " ");
        }
        towrite = sbuff.toString();
        //reading directories is also progress
        reporter.progress();
      }
      else {
        FSDataInputStream input = srcFs.open(srcStatus.getPath());
        reporter.setStatus("Copying file " + srcStatus.getPath() + 
            " to archive.");
        copyData(srcStatus.getPath(), input, partStream, reporter);
        towrite = encodeName(relPath.toString())
                  + " file " + partname + " " + startPos
                  + " " + srcStatus.getLen() + " " + propStr + " ";
      }
      out.collect(new IntWritable(hash), new Text(towrite));
    }
    
    public void close() throws IOException {
      // close the part files.
      partStream.close();
      destFs.setReplication(tmpOutput, (short) replication);
    }
  }
  
  /** the reduce for creating the index and the master index 
   * 
   */
  static class HArchivesReducer implements Reducer<IntWritable, 
  Text, Text, Text> {
    private JobConf conf = null;
    private long startIndex = 0;
    private long endIndex = 0;
    private long startPos = 0;
    private Path masterIndex = null;
    private Path index = null;
    private FileSystem fs = null;
    private FSDataOutputStream outStream = null;
    private FSDataOutputStream indexStream = null;
    private int numIndexes = 1000;
    private Path tmpOutputDir = null;
    private int written = 0;
    private int replication = 3;
    private int keyVal = 0;
    
    // configure 
    public void configure(JobConf conf) {
      this.conf = conf;
      tmpOutputDir = FileOutputFormat.getWorkOutputPath(this.conf);
      masterIndex = new Path(tmpOutputDir, "_masterindex");
      index = new Path(tmpOutputDir, "_index");
      replication = conf.getInt(HAR_REPLICATION_LABEL, 3);
      try {
        fs = masterIndex.getFileSystem(conf);
        fs.delete(masterIndex, false);
        fs.delete(index, false);
        indexStream = fs.create(index);
        outStream = fs.create(masterIndex);
        String version = VERSION + " \n";
        outStream.write(version.getBytes(Charsets.UTF_8));
        
      } catch(IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    // create the index and master index. The input to 
    // the reduce is already sorted by the hash of the 
    // files. SO we just need to write it to the index. 
    // We update the masterindex as soon as we update 
    // numIndex entries.
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<Text, Text> out,
        Reporter reporter) throws IOException {
      keyVal = key.get();
      while(values.hasNext()) {
        Text value = values.next();
        String towrite = value.toString() + "\n";
        indexStream.write(towrite.getBytes(Charsets.UTF_8));
        written++;
        if (written > numIndexes -1) {
          // every 1000 indexes we report status
          reporter.setStatus("Creating index for archives");
          reporter.progress();
          endIndex = keyVal;
          String masterWrite = startIndex + " " + endIndex + " " + startPos 
                              +  " " + indexStream.getPos() + " \n" ;
          outStream.write(masterWrite.getBytes(Charsets.UTF_8));
          startPos = indexStream.getPos();
          startIndex = endIndex;
          written = 0;
        }
      }
    }
    
    public void close() throws IOException {
      //write the last part of the master index.
      if (written > 0) {
        String masterWrite = startIndex + " " + keyVal + " " + startPos  +
                             " " + indexStream.getPos() + " \n";
        outStream.write(masterWrite.getBytes(Charsets.UTF_8));
      }
      // close the streams
      outStream.close();
      indexStream.close();
      // try increasing the replication 
      fs.setReplication(index, (short) replication);
      fs.setReplication(masterIndex, (short) replication);
    }
    
  }

  private void printUsage(Options opts, boolean printDetailed) {
    HelpFormatter helpFormatter = new HelpFormatter();
    if (printDetailed) {
      helpFormatter.printHelp(usage.length() + 10, usage, null, opts, null,
          false);
    } else {
      System.out.println(usage);
    }
  }

  /** the main driver for creating the archives
   *  it takes at least three command line parameters. The parent path, 
   *  The src and the dest. It does an lsr on the source paths.
   *  The mapper created archuves and the reducer creates 
   *  the archive index.
   */

  public int run(String[] args) throws Exception {
    try {
      // Parse CLI options
      Options options = new Options();
      options.addOption(ARCHIVE_NAME, true,
          "Name of the Archive. This is mandatory option");
      options.addOption(PARENT_PATH, true,
          "Parent path of sources. This is mandatory option");
      options.addOption(REPLICATION, true, "Replication factor archive files");
      options.addOption(HELP, false, "Show the usage");
      Parser parser = new GnuParser();
      CommandLine commandLine = parser.parse(options, args, true);

      if (commandLine.hasOption(HELP)) {
        printUsage(options, true);
        return 0;
      }
      if (!commandLine.hasOption(ARCHIVE_NAME)) {
        printUsage(options, false);
        throw new IOException("Archive Name not specified.");
      }
      String archiveName = commandLine.getOptionValue(ARCHIVE_NAME);
      if (!checkValidName(archiveName)) {
        printUsage(options, false);
        throw new IOException("Invalid name for archives. " + archiveName);
      }
      //check to see if relative parent has been provided or not
      //this is a required parameter. 
      if (!commandLine.hasOption(PARENT_PATH)) {
        printUsage(options, false);
        throw new IOException("Parent path not specified.");
      }
      Path parentPath = new Path(commandLine.getOptionValue(PARENT_PATH));
      if (!parentPath.isAbsolute()) {
        parentPath = parentPath.getFileSystem(getConf()).makeQualified(
            parentPath);
      }

      if (commandLine.hasOption(REPLICATION)) {
        repl = Short.parseShort(commandLine.getOptionValue(REPLICATION));
      }
      // Remaining args
      args = commandLine.getArgs();
      List<Path> srcPaths = new ArrayList<Path>();
      Path destPath = null;
      //read the rest of the paths
      for (int i = 0; i < args.length; i++) {
        if (i == (args.length - 1)) {
          destPath = new Path(args[i]);
          if (!destPath.isAbsolute()) {
            destPath = destPath.getFileSystem(getConf()).makeQualified(destPath);
          }
        }
        else {
          Path argPath = new Path(args[i]);
          if (argPath.isAbsolute()) {
            printUsage(options, false);
            throw new IOException("Source path " + argPath +
                " is not relative to "+ parentPath);
          }
          srcPaths.add(new Path(parentPath, argPath));
        }
      }
      if (destPath == null) {
        printUsage(options, false);
        throw new IOException("Destination path not specified.");
      }
      if (srcPaths.size() == 0) {
        // assuming if the user does not specify path for sources
        // the whole parent directory needs to be archived. 
        srcPaths.add(parentPath);
      }
      // do a glob on the srcPaths and then pass it on
      List<Path> globPaths = new ArrayList<Path>();
      for (Path p: srcPaths) {
        FileSystem fs = p.getFileSystem(getConf());
        FileStatus[] statuses = fs.globStatus(p);
        if (statuses != null) {
          for (FileStatus status: statuses) {
            globPaths.add(fs.makeQualified(status.getPath()));
          }
        }
      }
      if (globPaths.isEmpty()) {
        throw new IOException("The resolved paths set is empty."
            + "  Please check whether the srcPaths exist, where srcPaths = "
            + srcPaths);
      }

      archive(parentPath, globPaths, archiveName, destPath);
    } catch(IOException ie) {
      System.err.println(ie.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  static final String TEST_HADOOP_ARCHIVES_JAR_PATH = "test.hadoop.archives.jar";

  /** the main functions **/
  public static void main(String[] args) {
    JobConf job = new JobConf(HadoopArchives.class);

    HadoopArchives harchives = new HadoopArchives(job);
    int ret = 0;

    try{
      ret = ToolRunner.run(harchives, args);
    } catch(Exception e) {
      LOG.debug("Exception in archives  ", e);
      System.err.println(e.getClass().getSimpleName() + " in archives");
      final String s = e.getLocalizedMessage();
      if (s != null) {
        System.err.println(s);
      } else {
        e.printStackTrace(System.err);
      }
      System.exit(1);
    }
    System.exit(ret);
  }
}
