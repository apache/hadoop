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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/**
 * A Map-reduce program to recursively copy directories between
 * different file-systems.
 */
public class CopyFiles implements Tool {
  private static final Log LOG = LogFactory.getLog(CopyFiles.class);

  private static final String NAME = "distcp";

  private static final String usage = NAME
    + " [OPTIONS] <srcurl>* <desturl>" +
    "\n\nOPTIONS:" +
    "\n-p                     Preserve status" +
    "\n-i                     Ignore failures" +
    "\n-log <logdir>          Write logs to <logdir>" +
    "\n-overwrite             Overwrite destination" +
    "\n-update                Overwrite if src size different from dst size" +
    "\n-f <urilist_uri>       Use list at <urilist_uri> as src list" +
    "\n\nNOTE: if -overwrite or -update are set, each source URI is " +
    "\n      interpreted as an isomorphic update to an existing directory." +
    "\nFor example:" +
    "\nhadoop " + NAME + " -p -update \"hdfs://A:8020/user/foo/bar\" " +
    "\"hdfs://B:8020/user/foo/baz\"\n" +
    "\n     would update all descendants of 'baz' also in 'bar'; it would " +
    "\n     *not* update /user/foo/baz/bar\n";

  private static final long BYTES_PER_MAP =  256 * 1024 * 1024;
  private static final int MAX_MAPS_PER_NODE = 20;
  private static final int SYNC_FILE_MAX = 10;

  static enum Counter { COPY, SKIP, FAIL, BYTESCOPIED, BYTESEXPECTED }
  static enum Options {
    IGNORE_READ_FAILURES("-i", NAME + ".ignore.read.failures"),
    PRESERVE_STATUS("-p", NAME + ".preserve.status.info"),
    OVERWRITE("-overwrite", NAME + ".overwrite.always"),
    UPDATE("-update", NAME + ".overwrite.ifnewer");

    final String cmd, propertyname;

    private Options(String cmd, String propertyname) {
      this.cmd = cmd;
      this.propertyname = propertyname;
    }
  }

  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  static final String DST_DIR_LABEL = NAME + ".dest.path";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String SRC_COUNT_LABEL = NAME + ".src.count";
  static final String TOTAL_SIZE_LABEL = NAME + ".total.size";

  private JobConf conf;

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public CopyFiles(Configuration conf) {
    setConf(conf);
  }

  /**
   * An input/output pair of filenames.
   */
  static class FilePair implements Writable {
    FileStatus input = new FileStatus();
    Path output;
    FilePair() { }
    FilePair(FileStatus input, Path output) {
      this.input = input;
      this.output = output;
    }
    public void readFields(DataInput in) throws IOException {
      input.readFields(in);
      output = new Path(Text.readString(in));
    }
    public void write(DataOutput out) throws IOException {
      input.write(out);
      Text.writeString(out, output.toString());
    }
  }

  /**
   * InputFormat of a distcp job responsible for generating splits of the src
   * file list.
   */
  static class CopyInputFormat implements InputFormat<Text, Text> {

    /**
     * Does nothing.
     */
    public void validateInput(JobConf job) throws IOException { }

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     * @param job The handle to the JobConf object
     * @param numSplits Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      int cnfiles = job.getInt(SRC_COUNT_LABEL, -1);
      long cbsize = job.getLong(TOTAL_SIZE_LABEL, -1);
      String srcfilelist = job.get(SRC_LIST_LABEL, "");
      if (cnfiles < 0 || cbsize < 0 || "".equals(srcfilelist)) {
        throw new RuntimeException("Invalid metadata: #files(" + cnfiles +
                                   ") total_size(" + cbsize + ") listuri(" +
                                   srcfilelist + ")");
      }
      Path src = new Path(srcfilelist);
      FileSystem fs = src.getFileSystem(job);
      FileStatus srcst = fs.getFileStatus(src);

      ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      LongWritable key = new LongWritable();
      FilePair value = new FilePair();
      final long targetsize = cbsize / numSplits;
      long pos = 0L;
      long last = 0L;
      long acc = 0L;
      long cbrem = srcst.getLen();
      SequenceFile.Reader sl = null;
      try {
        sl = new SequenceFile.Reader(fs, src, job);
        for (; sl.next(key, value); last = sl.getPosition()) {
          // if adding this split would put this split past the target size,
          // cut the last split and put this next file in the next split.
          if (acc + key.get() > targetsize && acc != 0) {
            long splitsize = last - pos;
            splits.add(new FileSplit(src, pos, splitsize, job));
            cbrem -= splitsize;
            pos = last;
            acc = 0L;
          }
          acc += key.get();
        }
      }
      finally {
        checkAndClose(sl);
      }
      if (cbrem != 0) {
        splits.add(new FileSplit(src, pos, cbrem, job));
      }

      return splits.toArray(new FileSplit[splits.size()]);
    }

    /**
     * Returns a reader for this split of the src file list.
     */
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, Text>(job, (FileSplit)split);
    }
  }

  /**
   * FSCopyFilesMapper: The mapper for copying files between FileSystems.
   */
  public static class FSCopyFilesMapper
      implements Mapper<LongWritable, FilePair, WritableComparable, Text> {
    // config
    private int sizeBuf = 128 * 1024;
    private FileSystem destFileSys = null;
    private boolean ignoreReadFailures;
    private boolean preserve_status;
    private boolean overwrite;
    private boolean update;
    private Path destPath = null;
    private byte[] buffer = null;
    private JobConf job;

    // stats
    private static final DecimalFormat pcntfmt = new DecimalFormat("0.00");
    private int failcount = 0;
    private int skipcount = 0;
    private int copycount = 0;

    private void updateStatus(Reporter reporter) {
      reporter.setStatus("Copied: " + copycount + " Skipped: " + skipcount +
                    " Failed: " + failcount);
    }

    /**
     * Return true if dst should be replaced by src and the update flag is set.
     * Right now, this merely checks that the src and dst len are not equal. 
     * This should be improved on once modification times, CRCs, etc. can
     * be meaningful in this context.
     */
    private boolean needsUpdate(FileStatus src, FileStatus dst) {
      return update && src.getLen() != dst.getLen();
    }

    /**
     * Copy a file to a destination.
     * @param srcstat src path and metadata
     * @param dstpath dst path
     * @param reporter
     */
    private void copy(FileStatus srcstat, Path relativedst,
        OutputCollector<WritableComparable, Text> outc, Reporter reporter)
        throws IOException {
      Path absdst = new Path(destPath, relativedst);
      int totfiles = job.getInt(SRC_COUNT_LABEL, -1);
      assert totfiles >= 0 : "Invalid file count " + totfiles;

      // if a directory, ensure created even if empty
      if (srcstat.isDir()) {
        if (destFileSys.exists(absdst)) {
          if (!destFileSys.getFileStatus(absdst).isDir()) {
            throw new IOException("Failed to mkdirs: " + absdst+" is a file.");
          }
        }
        else if (!destFileSys.mkdirs(absdst)) {
          throw new IOException("Failed to mkdirs " + absdst);
        }
        // TODO: when modification times can be set, directories should be
        // emitted to reducers so they might be preserved. Also, mkdirs does
        // not currently return an error when the directory already exists;
        // if this changes, all directory work might as well be done in reduce
        return;
      }

      if (destFileSys.exists(absdst) && !overwrite
          && !needsUpdate(srcstat, destFileSys.getFileStatus(absdst))) {
        outc.collect(null, new Text("SKIP: " + srcstat.getPath()));
        ++skipcount;
        reporter.incrCounter(Counter.SKIP, 1);
        updateStatus(reporter);
        return;
      }

      Path tmpfile = new Path(job.get(TMP_DIR_LABEL), relativedst);
      long cbcopied = 0L;
      FSDataInputStream in = null;
      FSDataOutputStream out = null;
      try {
        // open src file
        in = srcstat.getPath().getFileSystem(job).open(srcstat.getPath());
        reporter.incrCounter(Counter.BYTESEXPECTED, srcstat.getLen());
        // open tmp file
        out = preserve_status
          ? destFileSys.create(tmpfile, true, sizeBuf, srcstat.getReplication(),
             srcstat.getBlockSize(), reporter)
          : destFileSys.create(tmpfile, reporter);
        // copy file
        int cbread;
        while ((cbread = in.read(buffer)) >= 0) {
          out.write(buffer, 0, cbread);
          cbcopied += cbread;
          reporter.setStatus(pcntfmt.format(100.0 * cbcopied / srcstat.getLen())
              + " " + absdst + " [ " +
              StringUtils.humanReadableInt(cbcopied) + " / " +
              StringUtils.humanReadableInt(srcstat.getLen()) + " ]");
        }
      } finally {
        checkAndClose(in);
        checkAndClose(out);
      }

      final boolean success = cbcopied == srcstat.getLen();
      if (!success) {
        final String badlen = "ERROR? copied " + bytesString(cbcopied)
            + " but expected " + bytesString(srcstat.getLen()) 
            + " from " + srcstat.getPath();
        LOG.warn(badlen);
        outc.collect(null, new Text(badlen));
      }
      else {
        if (totfiles == 1) {
          // Copying a single file; use dst path provided by user as destination
          // rather than destination directory
          absdst = absdst.getParent();
        }
        rename(destFileSys, tmpfile, absdst);
      }

      // report at least once for each file
      ++copycount;
      reporter.incrCounter(Counter.BYTESCOPIED, cbcopied);
      reporter.incrCounter(Counter.COPY, 1);
      updateStatus(reporter);
    }
    
    /** rename tmp to dst, delete dst if already exists */
    private void rename(FileSystem fs, Path tmp, Path dst) throws IOException {
      try {
        if (fs.exists(dst)) {
          fs.delete(dst);
        }
        fs.rename(tmp, dst);
      }
      catch(IOException cause) {
        IOException ioe = new IOException("Fail to rename tmp file (=" + tmp 
            + ") to destination file (=" + dst + ")");
        ioe.initCause(cause);
        throw ioe;
      }
    }
    
    static String bytesString(long b) {
      return b + " bytes (" + StringUtils.humanReadableInt(b) + ")";
    }

    /** Mapper configuration.
     * Extracts source and destination file system, as well as
     * top-level paths on source and destination directories.
     * Gets the named file systems, to be used later in map.
     */
    public void configure(JobConf job)
    {
      destPath = new Path(job.get(DST_DIR_LABEL, "/"));
      try {
        destFileSys = destPath.getFileSystem(job);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to get the named file system.", ex);
      }
      sizeBuf = job.getInt("copy.buf.size", 128 * 1024);
      buffer = new byte[sizeBuf];
      ignoreReadFailures = job.getBoolean(Options.IGNORE_READ_FAILURES.propertyname, false);
      preserve_status = job.getBoolean(Options.PRESERVE_STATUS.propertyname, false);
      update = job.getBoolean(Options.UPDATE.propertyname, false);
      overwrite = !update && job.getBoolean(Options.OVERWRITE.propertyname, false);
      this.job = job;
    }

    /** Map method. Copies one file from source file system to destination.
     * @param key src len
     * @param value FilePair (FileStatus src, Path dst)
     * @param out Log of failed copies
     * @param reporter
     */
    public void map(LongWritable key,
                    FilePair value,
                    OutputCollector<WritableComparable, Text> out,
                    Reporter reporter) throws IOException {
      FileStatus srcstat = value.input;
      Path dstpath = value.output;
      try {
        copy(srcstat, dstpath, out, reporter);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FAIL, 1);
        updateStatus(reporter);
        final String sfailure = "FAIL " + dstpath + " : " +
                          StringUtils.stringifyException(e);
        out.collect(null, new Text(sfailure));
        LOG.info(sfailure);
        try {
          for (int i = 0; i < 3; ++i) {
            try {
              if (destFileSys.delete(dstpath))
                break;
            } catch (Throwable ex) {
              // ignore, we are just cleaning up
              LOG.debug("Ignoring cleanup exception", ex);
            }
            // update status, so we don't get timed out
            updateStatus(reporter);
            Thread.sleep(3 * 1000);
          }
        } catch (InterruptedException inte) {
          throw (IOException)new IOException().initCause(inte);
        }
      } finally {
        updateStatus(reporter);
      }
    }

    public void close() throws IOException {
      if (0 == failcount || ignoreReadFailures) {
        return;
      }
      throw new IOException("Copied: " + copycount + " Skipped: " + skipcount +
          " Failed: " + failcount);
    }

  }

  private static List<Path> fetchFileList(Configuration conf, Path srcList)
      throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = srcList.getFileSystem(conf);
    BufferedReader input = null;
    try {
      input = new BufferedReader(new InputStreamReader(fs.open(srcList)));
      String line = input.readLine();
      while (line != null) {
        result.add(new Path(line));
        line = input.readLine();
      }
    } finally {
      checkAndClose(input);
    }
    return result;
  }

  @Deprecated
  public static void copy(Configuration conf, String srcPath,
                          String destPath, Path logPath,
                          boolean srcAsList, boolean ignoreReadFailures)
      throws IOException {
    final Path src = new Path(srcPath);
    List<Path> tmp = new ArrayList<Path>();
    if (srcAsList) {
      tmp.addAll(fetchFileList(conf, src));
    } else {
      tmp.add(src);
    }
    EnumSet<Options> flags = ignoreReadFailures
      ? EnumSet.of(Options.IGNORE_READ_FAILURES)
      : EnumSet.noneOf(Options.class);
    copy(conf, tmp, new Path(destPath), logPath, flags);
  }

  /** Sanity check for srcPath */
  private static void checkSrcPath(Configuration conf, List<Path> srcPaths
      ) throws IOException {
    List<IOException> rslt = new ArrayList<IOException>();
    for (Path p : srcPaths) {
      FileSystem fs = p.getFileSystem(conf);
      if (!fs.exists(p)) {
        rslt.add(new IOException("Input source " + p + " does not exist."));
      }
    }
    if (!rslt.isEmpty()) {
      throw new InvalidInputException(rslt);
    }
  }

  /**
   * Driver to copy srcPath to destPath depending on required protocol.
   * @param srcPaths list of source paths
   * @param destPath Destination path
   * @param logPath Log output directory
   * @param flags Command-line flags
   */
  public static void copy(Configuration conf, List<Path> srcPaths,
      Path destPath, Path logPath,
      EnumSet<Options> flags) throws IOException {
    LOG.info("srcPaths=" + srcPaths);
    LOG.info("destPath=" + destPath);
    checkSrcPath(conf, srcPaths);

    JobConf job = createJobConf(conf);
    //Initialize the mapper
    try {
      setup(conf, job, srcPaths, destPath, logPath, flags);
      JobClient.runJob(job);
    } finally {
      //delete tmp
      fullyDelete(job.get(TMP_DIR_LABEL), job);
      //delete jobDirectory
      fullyDelete(job.get(JOB_DIR_LABEL), job);
    }
  }

  /**
   * This is the main driver for recursively copying directories
   * across file systems. It takes at least two cmdline parameters. A source
   * URL and a destination URL. It then essentially does an "ls -lR" on the
   * source URL, and writes the output in a round-robin manner to all the map
   * input files. The mapper actually copies the files allotted to it. The
   * reduce is empty.
   */
  public int run(String[] args) throws Exception {
    List<Path> srcPath = new ArrayList<Path>();
    Path destPath = null;
    Path logPath = null;
    EnumSet<Options> flags = EnumSet.noneOf(Options.class);

    for (int idx = 0; idx < args.length; idx++) {
      Options[] opt = Options.values();
      int i = 0;
      for(; i < opt.length && !opt[i].cmd.equals(args[idx]); i++);

      if (i < opt.length) {
        flags.add(opt[i]);
      }        
      else if ("-f".equals(args[idx])) {
        if (++idx ==  args.length) {
          System.out.println("urilist_uri not specified");
          System.out.println(usage);
          return -1;
        }
        srcPath.addAll(fetchFileList(conf, new Path(args[idx])));
      } else if ("-log".equals(args[idx])) {
        if (++idx ==  args.length) {
          System.out.println("logdir not specified");
          System.out.println(usage);
          return -1;
        }
        logPath = new Path(args[idx]);
      } else if ('-' == args[idx].codePointAt(0)) {
        System.out.println("Invalid switch " + args[idx]);
        System.out.println(usage);
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
      } else if (idx == args.length -1) {
        destPath = new Path(args[idx]);
      } else {
        srcPath.add(new Path(args[idx]));
      }
    }
    // mandatory command-line parameters
    if (srcPath.isEmpty() || destPath == null) {
      System.out.println("Missing " + (destPath == null ? "dst path" : "src"));
      System.out.println(usage);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    // incompatible command-line flags
    if (flags.contains(Options.OVERWRITE) && flags.contains(Options.UPDATE)) {
      System.out.println("Conflicting overwrite policies");
      System.out.println(usage);
      return -1;
    }
    try {
      copy(conf, srcPath, destPath, logPath, flags);
    } catch (DuplicationException e) {
      System.err.println(StringUtils.stringifyException(e));
      return DuplicationException.ERROR_CODE;
    } catch (Exception e) {
      System.err.println("With failures, global counters are inaccurate; " +
          "consider running with -i");
      System.err.println("Copy failed: " + StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf(CopyFiles.class);
    CopyFiles distcp = new CopyFiles(job);
    int res = ToolRunner.run(distcp, args);
    System.exit(res);
  }

  /**
   * Make a path relative with respect to a root path.
   * absPath is always assumed to descend from root.
   * Otherwise returned path is null.
   */
  public static Path makeRelative(Path root, Path absPath) {
    if (!absPath.isAbsolute()) { return absPath; }
    String sRoot = root.toUri().getPath();
    String sPath = absPath.toUri().getPath();
    Enumeration<Object> rootTokens = new StringTokenizer(sRoot, "/");
    ArrayList rList = Collections.list(rootTokens);
    Enumeration<Object> pathTokens = new StringTokenizer(sPath, "/");
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
   * Calculate how many maps to run.
   * Number of maps is bounded by a minimum of the cumulative size of the copy /
   * BYTES_PER_MAP and at most MAX_MAPS_PER_NODE * nodes in the
   * cluster.
   * @param totalBytes Count of total bytes for job
   * @param numNodes the number of nodes in cluster
   * @return Count of maps to run.
   */
  private static int getMapCount(long totalBytes, int numNodes) {
    int numMaps = (int)(totalBytes / BYTES_PER_MAP);
    numMaps = Math.min(numMaps, numNodes * MAX_MAPS_PER_NODE);
    return Math.max(numMaps, 1);
  }

  /** Fully delete dir */
  static void fullyDelete(String dir, Configuration conf) throws IOException {
    if (dir != null) {
      Path tmp = new Path(dir);
      FileUtil.fullyDelete(tmp.getFileSystem(conf), tmp);
    }
  }

  //Job configuration
  private static JobConf createJobConf(Configuration conf) {
    JobConf jobconf = new JobConf(conf, CopyFiles.class);
    jobconf.setJobName(NAME);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobconf.setMapSpeculativeExecution(false);

    jobconf.setInputFormat(CopyInputFormat.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setMapperClass(FSCopyFilesMapper.class);
    jobconf.setNumReduceTasks(0);
    return jobconf;
  }

  private static final Random RANDOM = new Random();
  private static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }

  private static boolean setBooleans(JobConf jobConf, EnumSet<Options> flags) {
    boolean update = flags.contains(Options.UPDATE);
    boolean overwrite = !update && flags.contains(Options.OVERWRITE);
    jobConf.setBoolean(Options.UPDATE.propertyname, update);
    jobConf.setBoolean(Options.OVERWRITE.propertyname, overwrite);
    jobConf.setBoolean(Options.IGNORE_READ_FAILURES.propertyname,
        flags.contains(Options.IGNORE_READ_FAILURES));
    jobConf.setBoolean(Options.PRESERVE_STATUS.propertyname,
        flags.contains(Options.PRESERVE_STATUS));
    return update || overwrite;
  }

  /**
   * Initialize DFSCopyFileMapper specific job-configuration.
   * @param conf : The dfs/mapred configuration.
   * @param jobConf : The handle to the jobConf object to be initialized.
   * @param srcPaths : The source URIs.
   * @param destPath : The destination URI.
   * @param logPath : Log output directory
   * @param flags : Command-line flags
   */
  private static void setup(Configuration conf, JobConf jobConf,
                            List<Path> srcPaths, final Path destPath,
                            Path logPath, EnumSet<Options> flags)
      throws IOException {
    jobConf.set(DST_DIR_LABEL, destPath.toUri().toString());
    final boolean updateORoverwrite = setBooleans(jobConf, flags);

    final String randomId = getRandomId();
    Path jobDirectory = new Path(jobConf.getSystemDir(), NAME + "_" + randomId);
    jobConf.set(JOB_DIR_LABEL, jobDirectory.toString());

    FileSystem dstfs = destPath.getFileSystem(conf);
    boolean dstExists = dstfs.exists(destPath);
    boolean dstIsDir = false;
    if (dstExists) {
      dstIsDir = dstfs.getFileStatus(destPath).isDir();
    }

    // default logPath
    if (logPath == null) {
      String filename = "_distcp_logs_" + randomId;
      if (!dstExists || !dstIsDir) {
        Path parent = destPath.getParent();
        dstfs.mkdirs(parent);
        logPath = new Path(parent, filename);
      } else {
        logPath = new Path(destPath, filename);
      }
    }
    jobConf.setOutputPath(logPath);
    
    // create src list, dst list
    FileSystem jobfs = jobDirectory.getFileSystem(jobConf);

    Path srcfilelist = new Path(jobDirectory, "_distcp_src_files");
    jobConf.set(SRC_LIST_LABEL, srcfilelist.toString());
    SequenceFile.Writer src_writer = SequenceFile.createWriter(jobfs, jobConf,
        srcfilelist, LongWritable.class, FilePair.class,
        SequenceFile.CompressionType.NONE);

    Path dstfilelist = new Path(jobDirectory, "_distcp_dst_files");
    SequenceFile.Writer dst_writer = SequenceFile.createWriter(jobfs, jobConf,
        dstfilelist, Text.class, Text.class,
        SequenceFile.CompressionType.NONE);

    // handle the case where the destination directory doesn't exist
    // and we've only a single src directory OR we're updating/overwriting
    // the contents of the destination directory.
    final boolean special =
      (srcPaths.size() == 1 && !dstExists) || updateORoverwrite;
    int srcCount = 0, cnsyncf = 0;
    long cbsize = 0L, cbsyncs = 0L;
    try {
      for (Path p : srcPaths) {
        FileSystem fs = p.getFileSystem(conf);
        boolean pIsDir = fs.getFileStatus(p).isDir();
        Path root = special && pIsDir? p: p.getParent();
        if (pIsDir) {
          ++srcCount;
        }

        Stack<Path> pathstack = new Stack<Path>();
        pathstack.push(p);
        while (!pathstack.empty()) {
          for (FileStatus stat : fs.listStatus(pathstack.pop())) {
            ++srcCount;

            if (stat.isDir()) {
              pathstack.push(stat.getPath());
            }
            else {
              ++cnsyncf;
              cbsyncs += stat.getLen();
              cbsize += stat.getLen();

              if (cnsyncf > SYNC_FILE_MAX || cbsyncs > BYTES_PER_MAP) {
                src_writer.sync();
                dst_writer.sync();
                cnsyncf = 0;
                cbsyncs = 0L;
              }
            }

            Path dst = makeRelative(root, stat.getPath());
            src_writer.append(new LongWritable(stat.isDir()? 0: stat.getLen()),
                new FilePair(stat, dst));
            dst_writer.append(new Text(dst.toString()),
                new Text(stat.getPath().toString()));
          }
        }
      }
    } finally {
      checkAndClose(src_writer);
      checkAndClose(dst_writer);
    }

    // create dest path dir if copying > 1 file
    if (!dstfs.exists(destPath)) {
      if (srcCount > 1 && !dstfs.mkdirs(destPath)) {
        throw new IOException("Failed to create" + destPath);
      }
    }
    
    checkDuplication(jobfs, dstfilelist,
        new Path(jobDirectory, "_distcp_sorted"), conf);

    Path tmpDir = new Path(
        (dstExists && !dstIsDir) || (!dstExists && srcCount == 1)?
        destPath.getParent(): destPath, "_distcp_tmp_" + randomId);
    jobConf.set(TMP_DIR_LABEL, tmpDir.toUri().toString());
    LOG.info("srcCount=" + srcCount);
    jobConf.setInt(SRC_COUNT_LABEL, srcCount);
    jobConf.setLong(TOTAL_SIZE_LABEL, cbsize);
    jobConf.setNumMapTasks(getMapCount(cbsize,
        new JobClient(jobConf).getClusterStatus().getTaskTrackers()));
  }

  static private void checkDuplication(FileSystem fs, Path file, Path sorted,
    Configuration conf) throws IOException {
    SequenceFile.Reader in = null;
    try {
      SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
        new Text.Comparator(), Text.class, conf);
      sorter.sort(file, sorted);
      in = new SequenceFile.Reader(fs, sorted, conf);

      Text prevdst = null, curdst = new Text();
      Text prevsrc = null, cursrc = new Text(); 
      for(; in.next(curdst, cursrc); ) {
        if (prevdst != null && curdst.equals(prevdst)) {
          throw new DuplicationException(
            "Invalid input, there are duplicated files in the sources: "
            + prevsrc + ", " + cursrc);
        }
        prevdst = curdst;
        curdst = new Text();
        prevsrc = cursrc;
        cursrc = new Text();
      }
    }
    finally {
      checkAndClose(in);
    }
  } 

  static boolean checkAndClose(java.io.Closeable io) {
    if (io != null) {
      try {
        io.close();
      }
      catch(IOException ioe) {
        LOG.warn(StringUtils.stringifyException(ioe));
        return false;
      }
    }
    return true;
  }

  /** An exception class for duplicated source files. */
  public static class DuplicationException extends IOException {
    private static final long serialVersionUID = 1L;
    /** Error code for this exception */
    public static final int ERROR_CODE = -2;
    DuplicationException(String message) {super(message);}
  }
}
