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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.FileOutputFormat;
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
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;

/**
 * A Map-reduce program to recursively copy directories between
 * different file-systems.
 */
public class DistCpV1 implements Tool {
  public static final Log LOG = LogFactory.getLog(DistCpV1.class);

  private static final String NAME = "distcp";

  private static final String usage = NAME
    + " [OPTIONS] <srcurl>* <desturl>" +
    "\n\nOPTIONS:" +
    "\n-p[rbugpt]             Preserve status" +
    "\n                       r: replication number" +
    "\n                       b: block size" +
    "\n                       u: user" + 
    "\n                       g: group" +
    "\n                       p: permission" +
    "\n                       t: modification and access times" +
    "\n                       -p alone is equivalent to -prbugpt" +
    "\n-i                     Ignore failures" +
    "\n-basedir <basedir>     Use <basedir> as the base directory when copying files from <srcurl>" +
    "\n-log <logdir>          Write logs to <logdir>" +
    "\n-m <num_maps>          Maximum number of simultaneous copies" +
    "\n-overwrite             Overwrite destination" +
    "\n-update                Overwrite if src size different from dst size" +
    "\n-skipcrccheck          Do not use CRC check to determine if src is " +
    "\n                       different from dest. Relevant only if -update" +
    "\n                       is specified" +
    "\n-f <urilist_uri>       Use list at <urilist_uri> as src list" +
    "\n-filelimit <n>         Limit the total number of files to be <= n" +
    "\n-sizelimit <n>         Limit the total size to be <= n bytes" +
    "\n-delete                Delete the files existing in the dst but not in src" +
    "\n-dryrun                Display count of files and total size of files" +
    "\n                        in src and then exit. Copy is not done at all." +
    "\n                        desturl should not be speicified with out -update." +
    "\n-mapredSslConf <f>     Filename of SSL configuration for mapper task" +
    
    "\n\nNOTE 1: if -overwrite or -update are set, each source URI is " +
    "\n      interpreted as an isomorphic update to an existing directory." +
    "\nFor example:" +
    "\nhadoop " + NAME + " -p -update \"hdfs://A:8020/user/foo/bar\" " +
    "\"hdfs://B:8020/user/foo/baz\"\n" +
    "\n     would update all descendants of 'baz' also in 'bar'; it would " +
    "\n     *not* update /user/foo/baz/bar" + 

    "\n\nNOTE 2: The parameter <n> in -filelimit and -sizelimit can be " +
    "\n     specified with symbolic representation.  For examples," +
    "\n       1230k = 1230 * 1024 = 1259520" +
    "\n       891g = 891 * 1024^3 = 956703965184" +
    
    "\n";
  
  private static final long BYTES_PER_MAP =  256 * 1024 * 1024;
  private static final int MAX_MAPS_PER_NODE = 20;
  private static final int SYNC_FILE_MAX = 10;
  private static final int DEFAULT_FILE_RETRIES = 3;

  static enum Counter { COPY, SKIP, FAIL, BYTESCOPIED, BYTESEXPECTED }
  static enum Options {
    DELETE("-delete", NAME + ".delete"),
    FILE_LIMIT("-filelimit", NAME + ".limit.file"),
    SIZE_LIMIT("-sizelimit", NAME + ".limit.size"),
    IGNORE_READ_FAILURES("-i", NAME + ".ignore.read.failures"),
    PRESERVE_STATUS("-p", NAME + ".preserve.status"),
    OVERWRITE("-overwrite", NAME + ".overwrite.always"),
    UPDATE("-update", NAME + ".overwrite.ifnewer"),
    SKIPCRC("-skipcrccheck", NAME + ".skip.crc.check");

    final String cmd, propertyname;

    private Options(String cmd, String propertyname) {
      this.cmd = cmd;
      this.propertyname = propertyname;
    }
    
    private long parseLong(String[] args, int offset) {
      if (offset ==  args.length) {
        throw new IllegalArgumentException("<n> not specified in " + cmd);
      }
      long n = StringUtils.TraditionalBinaryPrefix.string2long(args[offset]);
      if (n <= 0) {
        throw new IllegalArgumentException("n = " + n + " <= 0 in " + cmd);
      }
      return n;
    }
  }
  static enum FileAttribute {
    BLOCK_SIZE, REPLICATION, USER, GROUP, PERMISSION, TIMES;

    final char symbol;

    private FileAttribute() {
      symbol = StringUtils.toLowerCase(toString()).charAt(0);
    }
    
    static EnumSet<FileAttribute> parse(String s) {
      if (s == null || s.length() == 0) {
        return EnumSet.allOf(FileAttribute.class);
      }

      EnumSet<FileAttribute> set = EnumSet.noneOf(FileAttribute.class);
      FileAttribute[] attributes = values();
      for(char c : s.toCharArray()) {
        int i = 0;
        for(; i < attributes.length && c != attributes[i].symbol; i++);
        if (i < attributes.length) {
          if (!set.contains(attributes[i])) {
            set.add(attributes[i]);
          } else {
            throw new IllegalArgumentException("There are more than one '"
                + attributes[i].symbol + "' in " + s); 
          }
        } else {
          throw new IllegalArgumentException("'" + c + "' in " + s
              + " is undefined.");
        }
      }
      return set;
    }
  }

  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  static final String DST_DIR_LABEL = NAME + ".dest.path";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String MAX_MAPS_LABEL = NAME + ".max.map.tasks";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String SRC_COUNT_LABEL = NAME + ".src.count";
  static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
  static final String DST_DIR_LIST_LABEL = NAME + ".dst.dir.list";
  static final String BYTES_PER_MAP_LABEL = NAME + ".bytes.per.map";
  static final String PRESERVE_STATUS_LABEL
      = Options.PRESERVE_STATUS.propertyname + ".value";
  static final String FILE_RETRIES_LABEL = NAME + ".file.retries";

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

  public DistCpV1(Configuration conf) {
    setConf(conf);
  }

  /**
   * An input/output pair of filenames.
   */
  static class FilePair implements Writable {
    FileStatus input = new FileStatus();
    String output;
    FilePair() { }
    FilePair(FileStatus input, String output) {
      this.input = input;
      this.output = output;
    }
    public void readFields(DataInput in) throws IOException {
      input.readFields(in);
      output = Text.readString(in);
    }
    public void write(DataOutput out) throws IOException {
      input.write(out);
      Text.writeString(out, output);
    }
    public String toString() {
      return input + " : " + output;
    }
  }

  /**
   * InputFormat of a distcp job responsible for generating splits of the src
   * file list.
   */
  static class CopyInputFormat implements InputFormat<Text, Text> {

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
      try (SequenceFile.Reader sl =
          new SequenceFile.Reader(job, Reader.file(src))) {
        for (; sl.next(key, value); last = sl.getPosition()) {
          // if adding this split would put this split past the target size,
          // cut the last split and put this next file in the next split.
          if (acc + key.get() > targetsize && acc != 0) {
            long splitsize = last - pos;
            splits.add(new FileSplit(src, pos, splitsize, (String[])null));
            cbrem -= splitsize;
            pos = last;
            acc = 0L;
          }
          acc += key.get();
        }
      }
      if (cbrem != 0) {
        splits.add(new FileSplit(src, pos, cbrem, (String[])null));
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
  static class CopyFilesMapper
      implements Mapper<LongWritable, FilePair, WritableComparable<?>, Text> {
    // config
    private int sizeBuf = 128 * 1024;
    private FileSystem destFileSys = null;
    private boolean ignoreReadFailures;
    private boolean preserve_status;
    private EnumSet<FileAttribute> preseved;
    private boolean overwrite;
    private boolean update;
    private Path destPath = null;
    private byte[] buffer = null;
    private JobConf job;
    private boolean skipCRCCheck = false;
    
    // stats
    private int failcount = 0;
    private int skipcount = 0;
    private int copycount = 0;

    private String getCountString() {
      return "Copied: " + copycount + " Skipped: " + skipcount
          + " Failed: " + failcount;
    }
    private void updateStatus(Reporter reporter) {
      reporter.setStatus(getCountString());
    }

    /**
     * Return true if dst should be replaced by src and the update flag is set.
     * Right now, this merely checks that the src and dst len are not equal. 
     * This should be improved on once modification times, CRCs, etc. can
     * be meaningful in this context.
     * @throws IOException 
     */
    private boolean needsUpdate(FileStatus srcstatus,
        FileSystem dstfs, Path dstpath) throws IOException {
      return update && !sameFile(srcstatus.getPath().getFileSystem(job),
          srcstatus, dstfs, dstpath, skipCRCCheck);
    }
    
    private FSDataOutputStream create(Path f, Reporter reporter,
        FileStatus srcstat) throws IOException {
      if (destFileSys.exists(f)) {
        destFileSys.delete(f, false);
      }
      if (!preserve_status) {
        return destFileSys.create(f, true, sizeBuf, reporter);
      }

      FsPermission permission = preseved.contains(FileAttribute.PERMISSION)?
          srcstat.getPermission(): null;
      short replication = preseved.contains(FileAttribute.REPLICATION)?
          srcstat.getReplication(): destFileSys.getDefaultReplication(f);
      long blockSize = preseved.contains(FileAttribute.BLOCK_SIZE)?
          srcstat.getBlockSize(): destFileSys.getDefaultBlockSize(f);
      return destFileSys.create(f, permission, true, sizeBuf, replication,
          blockSize, reporter);
    }

    /**
     * Validates copy by checking the sizes of files first and then
     * checksums, if the filesystems support checksums.
     * @param srcstat src path and metadata
     * @param absdst dst path
     * @return true if src & destination files are same
     */
    private boolean validateCopy(FileStatus srcstat, Path absdst)
            throws IOException {
      if (destFileSys.exists(absdst)) {
        if (sameFile(srcstat.getPath().getFileSystem(job), srcstat,
            destFileSys, absdst, skipCRCCheck)) {
          return true;
        }
      }
      return false;
    }
    
    /**
     * Increment number of files copied and bytes copied and then report status
     */
    void updateCopyStatus(FileStatus srcstat, Reporter reporter) {
      copycount++;
      reporter.incrCounter(Counter.BYTESCOPIED, srcstat.getLen());
      reporter.incrCounter(Counter.COPY, 1);
      updateStatus(reporter);
    }
    
    /**
     * Skip copying this file if already exists at the destination.
     * Updates counters and copy status if skipping this file.
     * @return true    if copy of this file can be skipped
     */
    private boolean skipCopyFile(FileStatus srcstat, Path absdst,
                            OutputCollector<WritableComparable<?>, Text> outc,
                            Reporter reporter) throws IOException {
      if (destFileSys.exists(absdst) && !overwrite
          && !needsUpdate(srcstat, destFileSys, absdst)) {
        outc.collect(null, new Text("SKIP: " + srcstat.getPath()));
        ++skipcount;
        reporter.incrCounter(Counter.SKIP, 1);
        updateStatus(reporter);
        return true;
      }
      return false;
    }
    
    /**
     * Copies single file to the path specified by tmpfile.
     * @param srcstat  src path and metadata
     * @param tmpfile  temporary file to which copy is to be done
     * @param absdst   actual destination path to which copy is to be done
     * @param reporter
     * @return Number of bytes copied
     */
    private long doCopyFile(FileStatus srcstat, Path tmpfile, Path absdst,
                            Reporter reporter) throws IOException {
      long bytesCopied = 0L;
      Path srcPath = srcstat.getPath();
      // open src file
      try (FSDataInputStream in = srcPath.getFileSystem(job).open(srcPath)) {
        reporter.incrCounter(Counter.BYTESEXPECTED, srcstat.getLen());
        // open tmp file
        try (FSDataOutputStream out = create(tmpfile, reporter, srcstat)) {
          LOG.info("Copying file " + srcPath + " of size " +
                   srcstat.getLen() + " bytes...");
        
          // copy file
          for(int bytesRead; (bytesRead = in.read(buffer)) >= 0; ) {
            out.write(buffer, 0, bytesRead);
            bytesCopied += bytesRead;
            reporter.setStatus(
                String.format("%.2f ", bytesCopied*100.0/srcstat.getLen())
                + absdst + " [ " +
                TraditionalBinaryPrefix.long2String(bytesCopied, "", 1) + " / "
                + TraditionalBinaryPrefix.long2String(srcstat.getLen(), "", 1)
                + " ]");
          }
        }
      }
      return bytesCopied;
    }
    
    /**
     * Copy a file to a destination.
     * @param srcstat src path and metadata
     * @param relativedst relative dst path
     * @param outc Log of skipped files
     * @param reporter
     * @throws IOException if copy fails(even if the validation of copy fails)
     */
    private void copy(FileStatus srcstat, Path relativedst,
        OutputCollector<WritableComparable<?>, Text> outc, Reporter reporter)
        throws IOException {
      Path absdst = new Path(destPath, relativedst);
      int totfiles = job.getInt(SRC_COUNT_LABEL, -1);
      assert totfiles >= 0 : "Invalid file count " + totfiles;

      if (totfiles == 1) {
        // Copying a single file; use dst path provided by user as
        // destination file rather than destination directory
        Path dstparent = absdst.getParent();
        if (!(destFileSys.exists(dstparent) &&
              destFileSys.getFileStatus(dstparent).isDirectory())) {
          absdst = dstparent;
        }
      }
      
      // if a directory, ensure created even if empty
      if (srcstat.isDirectory()) {
        if (destFileSys.exists(absdst)) {
          if (destFileSys.getFileStatus(absdst).isFile()) {
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

      // Can we skip copying this file ?
      if (skipCopyFile(srcstat, absdst, outc, reporter)) {
        return;
      }

      Path tmpfile = new Path(job.get(TMP_DIR_LABEL), relativedst);
      // do the actual copy to tmpfile
      long bytesCopied = doCopyFile(srcstat, tmpfile, absdst, reporter);

      if (bytesCopied != srcstat.getLen()) {
        throw new IOException("File size not matched: copied "
            + bytesString(bytesCopied) + " to tmpfile (=" + tmpfile
            + ") but expected " + bytesString(srcstat.getLen()) 
            + " from " + srcstat.getPath());        
      }
      else {
        if (destFileSys.exists(absdst) &&
            destFileSys.getFileStatus(absdst).isDirectory()) {
          throw new IOException(absdst + " is a directory");
        }
        if (!destFileSys.mkdirs(absdst.getParent())) {
          throw new IOException("Failed to create parent dir: " + absdst.getParent());
        }
        rename(tmpfile, absdst);

        if (!validateCopy(srcstat, absdst)) {
          destFileSys.delete(absdst, false);
          throw new IOException("Validation of copy of file "
              + srcstat.getPath() + " failed.");
        } 
        updateDestStatus(srcstat, destFileSys.getFileStatus(absdst));
      }

      // report at least once for each file
      updateCopyStatus(srcstat, reporter);
    }
    
    /** rename tmp to dst, delete dst if already exists */
    private void rename(Path tmp, Path dst) throws IOException {
      try {
        if (destFileSys.exists(dst)) {
          destFileSys.delete(dst, true);
        }
        if (!destFileSys.rename(tmp, dst)) {
          throw new IOException();
        }
      }
      catch(IOException cause) {
        throw (IOException)new IOException("Fail to rename tmp file (=" + tmp 
            + ") to destination file (=" + dst + ")").initCause(cause);
      }
    }

    private void updateDestStatus(FileStatus src, FileStatus dst
        ) throws IOException {
      if (preserve_status) {
        DistCpV1.updateDestStatus(src, dst, preseved, destFileSys);
      }
    }

    static String bytesString(long b) {
      return b + " bytes (" +
          TraditionalBinaryPrefix.long2String(b, "", 1) + ")";
    }

    /**
     * Copies a file and validates the copy by checking the checksums.
     * If validation fails, retries (max number of tries is distcp.file.retries)
     * to copy the file.
     */
    void copyWithRetries(FileStatus srcstat, Path relativedst,
                         OutputCollector<WritableComparable<?>, Text> out,
                         Reporter reporter) throws IOException {

      // max tries to copy when validation of copy fails
      final int maxRetries = job.getInt(FILE_RETRIES_LABEL, DEFAULT_FILE_RETRIES);
      // save update flag for later copies within the same map task
      final boolean saveUpdate = update;
      
      int retryCnt = 1;
      for (; retryCnt <= maxRetries; retryCnt++) {
        try {
          //copy the file and validate copy
          copy(srcstat, relativedst, out, reporter);
          break;// copy successful
        } catch (IOException e) {
          LOG.warn("Copy of " + srcstat.getPath() + " failed.", e);
          if (retryCnt < maxRetries) {// copy failed and need to retry
            LOG.info("Retrying copy of file " + srcstat.getPath());
            update = true; // set update flag for retries
          }
          else {// no more retries... Give up
            update = saveUpdate;
            throw new IOException("Copy of file failed even with " + retryCnt
                                  + " tries.", e);
          }
        }
      }
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
      if (preserve_status) {
        preseved = FileAttribute.parse(job.get(PRESERVE_STATUS_LABEL));
      }
      update = job.getBoolean(Options.UPDATE.propertyname, false);
      overwrite = !update && job.getBoolean(Options.OVERWRITE.propertyname, false);
      skipCRCCheck = job.getBoolean(Options.SKIPCRC.propertyname, false);
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
                    OutputCollector<WritableComparable<?>, Text> out,
                    Reporter reporter) throws IOException {
      final FileStatus srcstat = value.input;
      final Path relativedst = new Path(value.output);
      try {
        copyWithRetries(srcstat, relativedst, out, reporter);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FAIL, 1);
        updateStatus(reporter);
        final String sfailure = "FAIL " + relativedst + " : " +
                          StringUtils.stringifyException(e);
        out.collect(null, new Text(sfailure));
        LOG.info(sfailure);
        if (e instanceof FileNotFoundException) {
          final String s = "Possible Cause for failure: Either the filesystem "
                           + srcstat.getPath().getFileSystem(job)
                           + " is not accessible or the file is deleted";
          LOG.error(s);
          out.collect(null, new Text(s));
        }

        try {
          for (int i = 0; i < 3; ++i) {
            try {
              final Path tmp = new Path(job.get(TMP_DIR_LABEL), relativedst);
              if (destFileSys.delete(tmp, true))
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
      throw new IOException(getCountString());
    }
  }

  private static List<Path> fetchFileList(Configuration conf, Path srcList)
      throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = srcList.getFileSystem(conf);
    try (BufferedReader input = new BufferedReader(new InputStreamReader(fs.open(srcList),
            Charset.forName("UTF-8")))) {
      String line = input.readLine();
      while (line != null) {
        result.add(new Path(line));
        line = input.readLine();
      }
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

    final Path dst = new Path(destPath);
    copy(conf, new Arguments(tmp, null, dst, logPath, flags, null,
        Long.MAX_VALUE, Long.MAX_VALUE, null, false));
  }

  /** Sanity check for srcPath */
  private static void checkSrcPath(JobConf jobConf, List<Path> srcPaths) 
  throws IOException {
    List<IOException> rslt = new ArrayList<IOException>();
    List<Path> unglobbed = new LinkedList<Path>();
    
    Path[] ps = new Path[srcPaths.size()];
    ps = srcPaths.toArray(ps);
    TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(), ps, jobConf);
    
    
    for (Path p : srcPaths) {
      FileSystem fs = p.getFileSystem(jobConf);
      FileStatus[] inputs = fs.globStatus(p);
      
      if(inputs != null && inputs.length > 0) {
        for (FileStatus onePath: inputs) {
          unglobbed.add(onePath.getPath());
        }
      } else {
        rslt.add(new IOException("Input source " + p + " does not exist."));
      }
    }
    if (!rslt.isEmpty()) {
      throw new InvalidInputException(rslt);
    }
    srcPaths.clear();
    srcPaths.addAll(unglobbed);
  }

  /**
   * Driver to copy srcPath to destPath depending on required protocol.
   * @param conf configuration
   * @param args arguments
   */
  static void copy(final Configuration conf, final Arguments args
      ) throws IOException {
    LOG.info("srcPaths=" + args.srcs);
    if (!args.dryrun || args.flags.contains(Options.UPDATE)) {
      LOG.info("destPath=" + args.dst);
    }

    JobConf job = createJobConf(conf);
    
    checkSrcPath(job, args.srcs);
    if (args.preservedAttributes != null) {
      job.set(PRESERVE_STATUS_LABEL, args.preservedAttributes);
    }
    if (args.mapredSslConf != null) {
      job.set("dfs.https.client.keystore.resource", args.mapredSslConf);
    }
    
    //Initialize the mapper
    try {
      if (setup(conf, job, args)) {
        JobClient.runJob(job);
      }
      if(!args.dryrun) {
        finalize(conf, job, args.dst, args.preservedAttributes);
      }
    } finally {
      if (!args.dryrun) {
        //delete tmp
        fullyDelete(job.get(TMP_DIR_LABEL), job);
      }
      //delete jobDirectory
      fullyDelete(job.get(JOB_DIR_LABEL), job);
    }
  }

  private static void updateDestStatus(FileStatus src, FileStatus dst,
      EnumSet<FileAttribute> preseved, FileSystem destFileSys
      ) throws IOException {
    String owner = null;
    String group = null;
    if (preseved.contains(FileAttribute.USER)
        && !src.getOwner().equals(dst.getOwner())) {
      owner = src.getOwner();
    }
    if (preseved.contains(FileAttribute.GROUP)
        && !src.getGroup().equals(dst.getGroup())) {
      group = src.getGroup();
    }
    if (owner != null || group != null) {
      destFileSys.setOwner(dst.getPath(), owner, group);
    }
    if (preseved.contains(FileAttribute.PERMISSION)
        && !src.getPermission().equals(dst.getPermission())) {
      destFileSys.setPermission(dst.getPath(), src.getPermission());
    }
    if (preseved.contains(FileAttribute.TIMES)) {
      destFileSys.setTimes(dst.getPath(), src.getModificationTime(), src.getAccessTime());
    }
  }

  static private void finalize(Configuration conf, JobConf jobconf,
      final Path destPath, String presevedAttributes) throws IOException {
    if (presevedAttributes == null) {
      return;
    }
    EnumSet<FileAttribute> preseved = FileAttribute.parse(presevedAttributes);
    if (!preseved.contains(FileAttribute.USER)
        && !preseved.contains(FileAttribute.GROUP)
        && !preseved.contains(FileAttribute.PERMISSION)) {
      return;
    }

    FileSystem dstfs = destPath.getFileSystem(conf);
    Path dstdirlist = new Path(jobconf.get(DST_DIR_LIST_LABEL));
    try (SequenceFile.Reader in =
        new SequenceFile.Reader(jobconf, Reader.file(dstdirlist))) {
      Text dsttext = new Text();
      FilePair pair = new FilePair(); 
      for(; in.next(dsttext, pair); ) {
        Path absdst = new Path(destPath, pair.output);
        updateDestStatus(pair.input, dstfs.getFileStatus(absdst),
            preseved, dstfs);
      }
    }
  }

  static class Arguments {
    final List<Path> srcs;
    final Path basedir;
    final Path dst;
    final Path log;
    final EnumSet<Options> flags;
    final String preservedAttributes;
    final long filelimit;
    final long sizelimit;
    final String mapredSslConf;
    final boolean dryrun;
    
    /**
     * Arguments for distcp
     * @param srcs List of source paths
     * @param basedir Base directory for copy
     * @param dst Destination path
     * @param log Log output directory
     * @param flags Command-line flags
     * @param preservedAttributes Preserved attributes 
     * @param filelimit File limit
     * @param sizelimit Size limit
     * @param mapredSslConf ssl configuration
     * @param dryrun
     */
    Arguments(List<Path> srcs, Path basedir, Path dst, Path log,
        EnumSet<Options> flags, String preservedAttributes,
        long filelimit, long sizelimit, String mapredSslConf,
        boolean dryrun) {
      this.srcs = srcs;
      this.basedir = basedir;
      this.dst = dst;
      this.log = log;
      this.flags = flags;
      this.preservedAttributes = preservedAttributes;
      this.filelimit = filelimit;
      this.sizelimit = sizelimit;
      this.mapredSslConf = mapredSslConf;
      this.dryrun = dryrun;
      
      if (LOG.isTraceEnabled()) {
        LOG.trace("this = " + this);
      }
    }

    static Arguments valueOf(String[] args, Configuration conf
        ) throws IOException {
      List<Path> srcs = new ArrayList<Path>();
      Path dst = null;
      Path log = null;
      Path basedir = null;
      EnumSet<Options> flags = EnumSet.noneOf(Options.class);
      String presevedAttributes = null;
      String mapredSslConf = null;
      long filelimit = Long.MAX_VALUE;
      long sizelimit = Long.MAX_VALUE;
      boolean dryrun = false;

      for (int idx = 0; idx < args.length; idx++) {
        Options[] opt = Options.values();
        int i = 0;
        for(; i < opt.length && !args[idx].startsWith(opt[i].cmd); i++);

        if (i < opt.length) {
          flags.add(opt[i]);
          if (opt[i] == Options.PRESERVE_STATUS) {
            presevedAttributes =  args[idx].substring(2);         
            FileAttribute.parse(presevedAttributes); //validation
          }
          else if (opt[i] == Options.FILE_LIMIT) {
            filelimit = Options.FILE_LIMIT.parseLong(args, ++idx);
          }
          else if (opt[i] == Options.SIZE_LIMIT) {
            sizelimit = Options.SIZE_LIMIT.parseLong(args, ++idx);
          }
        } else if ("-f".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("urilist_uri not specified in -f");
          }
          srcs.addAll(fetchFileList(conf, new Path(args[idx])));
        } else if ("-log".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("logdir not specified in -log");
          }
          log = new Path(args[idx]);
        } else if ("-basedir".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("basedir not specified in -basedir");
          }
          basedir = new Path(args[idx]);
        } else if ("-mapredSslConf".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("ssl conf file not specified in -mapredSslConf");
          }
          mapredSslConf = args[idx];
        } else if ("-dryrun".equals(args[idx])) {
          dryrun = true;
          dst = new Path("/tmp/distcp_dummy_dest");//dummy destination
        } else if ("-m".equals(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException("num_maps not specified in -m");
          }
          try {
            conf.setInt(MAX_MAPS_LABEL, Integer.parseInt(args[idx]));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid argument to -m: " +
                                               args[idx]);
          }
        } else if ('-' == args[idx].codePointAt(0)) {
          throw new IllegalArgumentException("Invalid switch " + args[idx]);
        } else if (idx == args.length -1 &&
                   (!dryrun || flags.contains(Options.UPDATE))) {
          dst = new Path(args[idx]);
        } else {
          srcs.add(new Path(args[idx]));
        }
      }
      // mandatory command-line parameters
      if (srcs.isEmpty() || dst == null) {
        throw new IllegalArgumentException("Missing "
            + (dst == null ? "dst path" : "src"));
      }
      // incompatible command-line flags
      final boolean isOverwrite = flags.contains(Options.OVERWRITE);
      final boolean isUpdate = flags.contains(Options.UPDATE);
      final boolean isDelete = flags.contains(Options.DELETE);
      final boolean skipCRC = flags.contains(Options.SKIPCRC);
      if (isOverwrite && isUpdate) {
        throw new IllegalArgumentException("Conflicting overwrite policies");
      }
      if (!isUpdate && skipCRC) {
        throw new IllegalArgumentException(
            Options.SKIPCRC.cmd + " is relevant only with the " +
            Options.UPDATE.cmd + " option");
      }
      if (isDelete && !isOverwrite && !isUpdate) {
        throw new IllegalArgumentException(Options.DELETE.cmd
            + " must be specified with " + Options.OVERWRITE + " or "
            + Options.UPDATE + ".");
      }
      return new Arguments(srcs, basedir, dst, log, flags, presevedAttributes,
          filelimit, sizelimit, mapredSslConf, dryrun);
    }
    
    /** {@inheritDoc} */
    public String toString() {
      return getClass().getName() + "{"
          + "\n  srcs = " + srcs 
          + "\n  dst = " + dst 
          + "\n  log = " + log 
          + "\n  flags = " + flags
          + "\n  preservedAttributes = " + preservedAttributes 
          + "\n  filelimit = " + filelimit 
          + "\n  sizelimit = " + sizelimit
          + "\n  mapredSslConf = " + mapredSslConf
          + "\n}"; 
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
  public int run(String[] args) {
    try {
      copy(conf, Arguments.valueOf(args, conf));
      return 0;
    } catch (IllegalArgumentException e) {
      System.err.println(StringUtils.stringifyException(e) + "\n" + usage);
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    } catch (DuplicationException e) {
      System.err.println(StringUtils.stringifyException(e));
      return DuplicationException.ERROR_CODE;
    } catch (RemoteException e) {
      final IOException unwrapped = e.unwrapRemoteException(
          FileNotFoundException.class, 
          AccessControlException.class,
          QuotaExceededException.class);
      System.err.println(StringUtils.stringifyException(unwrapped));
      return -3;
    } catch (Exception e) {
      System.err.println("With failures, global counters are inaccurate; " +
          "consider running with -i");
      System.err.println("Copy failed: " + StringUtils.stringifyException(e));
      return -999;
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf(DistCpV1.class);
    DistCpV1 distcp = new DistCpV1(job);
    int res = ToolRunner.run(distcp, args);
    System.exit(res);
  }

  /**
   * Make a path relative with respect to a root path.
   * absPath is always assumed to descend from root.
   * Otherwise returned path is null.
   */
  static String makeRelative(Path root, Path absPath) {
    if (!absPath.isAbsolute()) {
      throw new IllegalArgumentException("!absPath.isAbsolute(), absPath="
          + absPath);
    }
    String p = absPath.toUri().getPath();

    StringTokenizer pathTokens = new StringTokenizer(p, "/");
    for(StringTokenizer rootTokens = new StringTokenizer(
        root.toUri().getPath(), "/"); rootTokens.hasMoreTokens(); ) {
      if (!rootTokens.nextToken().equals(pathTokens.nextToken())) {
        return null;
      }
    }
    StringBuilder sb = new StringBuilder();
    for(; pathTokens.hasMoreTokens(); ) {
      sb.append(pathTokens.nextToken());
      if (pathTokens.hasMoreTokens()) { sb.append(Path.SEPARATOR); }
    }
    return sb.length() == 0? ".": sb.toString();
  }

  /**
   * Calculate how many maps to run.
   * Number of maps is bounded by a minimum of the cumulative size of the
   * copy / (distcp.bytes.per.map, default BYTES_PER_MAP or -m on the
   * command line) and at most (distcp.max.map.tasks, default
   * MAX_MAPS_PER_NODE * nodes in the cluster).
   * @param totalBytes Count of total bytes for job
   * @param job The job to configure
   * @return Count of maps to run.
   */
  private static int setMapCount(long totalBytes, JobConf job) 
      throws IOException {
    int numMaps =
      (int)(totalBytes / job.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP));
    numMaps = Math.min(numMaps, 
        job.getInt(MAX_MAPS_LABEL, MAX_MAPS_PER_NODE *
          new JobClient(job).getClusterStatus().getTaskTrackers()));
    numMaps = Math.max(numMaps, 1);
    job.setNumMapTasks(numMaps);
    return numMaps;
  }

  /** Fully delete dir */
  static void fullyDelete(String dir, Configuration conf) throws IOException {
    if (dir != null) {
      Path tmp = new Path(dir);
      boolean success = tmp.getFileSystem(conf).delete(tmp, true);
      if (!success) {
        LOG.warn("Could not fully delete " + tmp);
      }
    }
  }

  //Job configuration
  private static JobConf createJobConf(Configuration conf) {
    JobConf jobconf = new JobConf(conf, DistCpV1.class);
    jobconf.setJobName(conf.get("mapred.job.name", NAME));

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobconf.setMapSpeculativeExecution(false);

    jobconf.setInputFormat(CopyInputFormat.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setMapperClass(CopyFilesMapper.class);
    jobconf.setNumReduceTasks(0);
    return jobconf;
  }

  private static final Random RANDOM = new Random();
  public static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }

  /**
   * Increase the replication factor of _distcp_src_files to
   * sqrt(min(maxMapsOnCluster, numMaps)). This is to reduce the chance of
   * failing of distcp because of "not having a replication of _distcp_src_files
   * available for reading for some maps".
   */
  private static void setReplication(Configuration conf, JobConf jobConf,
                         Path srcfilelist, int numMaps) throws IOException {
    int numMaxMaps = new JobClient(jobConf).getClusterStatus().getMaxMapTasks();
    short replication = (short) Math.ceil(
                                Math.sqrt(Math.min(numMaxMaps, numMaps)));
    FileSystem fs = srcfilelist.getFileSystem(conf);
    FileStatus srcStatus = fs.getFileStatus(srcfilelist);

    if (srcStatus.getReplication() < replication) {
      if (!fs.setReplication(srcfilelist, replication)) {
        throw new IOException("Unable to increase the replication of file " +
                              srcfilelist);
      }
    }
  }
  
  /**
   * Does the dir already exist at destination ?
   * @return true   if the dir already exists at destination
   */
  private static boolean dirExists(Configuration conf, Path dst)
                 throws IOException {
    FileSystem destFileSys = dst.getFileSystem(conf);
    FileStatus status = null;
    try {
      status = destFileSys.getFileStatus(dst);
    }catch (FileNotFoundException e) {
      return false;
    }
    if (status.isFile()) {
      throw new FileAlreadyExistsException("Not a dir: " + dst+" is a file.");
    }
    return true;
  }
  
  /**
   * Initialize DFSCopyFileMapper specific job-configuration.
   * @param conf : The dfs/mapred configuration.
   * @param jobConf : The handle to the jobConf object to be initialized.
   * @param args Arguments
   * @return true if it is necessary to launch a job.
   */
  static boolean setup(Configuration conf, JobConf jobConf,
                            final Arguments args)
      throws IOException {
    jobConf.set(DST_DIR_LABEL, args.dst.toUri().toString());

    //set boolean values
    final boolean update = args.flags.contains(Options.UPDATE);
    final boolean skipCRCCheck = args.flags.contains(Options.SKIPCRC);
    final boolean overwrite = !update && args.flags.contains(Options.OVERWRITE)
                              && !args.dryrun;
    jobConf.setBoolean(Options.UPDATE.propertyname, update);
    jobConf.setBoolean(Options.SKIPCRC.propertyname, skipCRCCheck);
    jobConf.setBoolean(Options.OVERWRITE.propertyname, overwrite);
    jobConf.setBoolean(Options.IGNORE_READ_FAILURES.propertyname,
        args.flags.contains(Options.IGNORE_READ_FAILURES));
    jobConf.setBoolean(Options.PRESERVE_STATUS.propertyname,
        args.flags.contains(Options.PRESERVE_STATUS));

    final String randomId = getRandomId();
    JobClient jClient = new JobClient(jobConf);
    Path stagingArea;
    try {
      stagingArea = 
        JobSubmissionFiles.getStagingDir(jClient.getClusterHandle(), conf);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    
    Path jobDirectory = new Path(stagingArea + NAME + "_" + randomId);
    FsPermission mapredSysPerms = 
      new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jClient.getFs(), jobDirectory, mapredSysPerms);
    jobConf.set(JOB_DIR_LABEL, jobDirectory.toString());

    long maxBytesPerMap = conf.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP);

    FileSystem dstfs = args.dst.getFileSystem(conf);
    
    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(), 
                                        new Path[] {args.dst}, conf);
    
    
    boolean dstExists = dstfs.exists(args.dst);
    boolean dstIsDir = false;
    if (dstExists) {
      dstIsDir = dstfs.getFileStatus(args.dst).isDirectory();
    }

    // default logPath
    Path logPath = args.log; 
    if (logPath == null) {
      String filename = "_distcp_logs_" + randomId;
      if (!dstExists || !dstIsDir) {
        Path parent = args.dst.getParent();
        if (null == parent) {
          // If dst is '/' on S3, it might not exist yet, but dst.getParent()
          // will return null. In this case, use '/' as its own parent to prevent
          // NPE errors below.
          parent = args.dst;
        }
        if (!dstfs.exists(parent)) {
          dstfs.mkdirs(parent);
        }
        logPath = new Path(parent, filename);
      } else {
        logPath = new Path(args.dst, filename);
      }
    }
    FileOutputFormat.setOutputPath(jobConf, logPath);

    // create src list, dst list
    FileSystem jobfs = jobDirectory.getFileSystem(jobConf);

    Path srcfilelist = new Path(jobDirectory, "_distcp_src_files");
    Path dstfilelist = new Path(jobDirectory, "_distcp_dst_files");
    Path dstdirlist = new Path(jobDirectory, "_distcp_dst_dirs");
    jobConf.set(SRC_LIST_LABEL, srcfilelist.toString());
    jobConf.set(DST_DIR_LIST_LABEL, dstdirlist.toString());
    int srcCount = 0, cnsyncf = 0, dirsyn = 0;
    long fileCount = 0L, dirCount = 0L, byteCount = 0L, cbsyncs = 0L,
         skipFileCount = 0L, skipByteCount = 0L;
    try (
        SequenceFile.Writer src_writer = SequenceFile.createWriter(jobConf,
            Writer.file(srcfilelist), Writer.keyClass(LongWritable.class),
            Writer.valueClass(FilePair.class), Writer.compression(
            SequenceFile.CompressionType.NONE));
        SequenceFile.Writer dst_writer = SequenceFile.createWriter(jobConf,
            Writer.file(dstfilelist), Writer.keyClass(Text.class),
            Writer.valueClass(Text.class), Writer.compression(
            SequenceFile.CompressionType.NONE));
        SequenceFile.Writer dir_writer = SequenceFile.createWriter(jobConf,
            Writer.file(dstdirlist), Writer.keyClass(Text.class),
            Writer.valueClass(FilePair.class), Writer.compression(
            SequenceFile.CompressionType.NONE));
    ) {
      // handle the case where the destination directory doesn't exist
      // and we've only a single src directory OR we're updating/overwriting
      // the contents of the destination directory.
      final boolean special =
        (args.srcs.size() == 1 && !dstExists) || update || overwrite;

      Path basedir = null;
      HashSet<Path> parentDirsToCopy = new HashSet<Path>();
      if (args.basedir != null) {
        FileSystem basefs = args.basedir.getFileSystem(conf);
        basedir = args.basedir.makeQualified(
            basefs.getUri(), basefs.getWorkingDirectory());
        if (!basefs.isDirectory(basedir)) {
          throw new IOException("Basedir " + basedir + " is not a directory.");
        }
      }

      for(Iterator<Path> srcItr = args.srcs.iterator(); srcItr.hasNext(); ) {
        final Path src = srcItr.next();
        FileSystem srcfs = src.getFileSystem(conf);
        FileStatus srcfilestat = srcfs.getFileStatus(src);
        Path root = special && srcfilestat.isDirectory()? src: src.getParent();
        if (dstExists && !dstIsDir &&
            (args.srcs.size() > 1 || srcfilestat.isDirectory())) {
          // destination should not be a file
          throw new IOException("Destination " + args.dst + " should be a dir" +
                                " if multiple source paths are there OR if" +
                                " the source path is a dir");
        }

        if (basedir != null) {
          root = basedir;
          Path parent = src.getParent().makeQualified(
              srcfs.getUri(), srcfs.getWorkingDirectory());
          while (parent != null && !parent.equals(basedir)) {
            if (!parentDirsToCopy.contains(parent)){
              parentDirsToCopy.add(parent);
              String dst = makeRelative(root, parent);
              FileStatus pst = srcfs.getFileStatus(parent);
              src_writer.append(new LongWritable(0), new FilePair(pst, dst));
              dst_writer.append(new Text(dst), new Text(parent.toString()));
              dir_writer.append(new Text(dst), new FilePair(pst, dst));
              if (++dirsyn > SYNC_FILE_MAX) {
                dirsyn = 0;
                dir_writer.sync();                
              }
            }
            parent = parent.getParent();
          }
          
          if (parent == null) {
            throw new IOException("Basedir " + basedir + 
                " is not a prefix of source path " + src);
          }
        }
        
        if (srcfilestat.isDirectory()) {
          ++srcCount;
          final String dst = makeRelative(root,src);
          if (!update || !dirExists(conf, new Path(args.dst, dst))) {
            ++dirCount;
            src_writer.append(new LongWritable(0),
                              new FilePair(srcfilestat, dst));
          }
          dst_writer.append(new Text(dst), new Text(src.toString()));
        }

        Stack<FileStatus> pathstack = new Stack<FileStatus>();
        for(pathstack.push(srcfilestat); !pathstack.empty(); ) {
          FileStatus cur = pathstack.pop();
          FileStatus[] children = srcfs.listStatus(cur.getPath());
          for(int i = 0; i < children.length; i++) {
            boolean skipPath = false;
            final FileStatus child = children[i]; 
            final String dst = makeRelative(root, child.getPath());
            ++srcCount;

            if (child.isDirectory()) {
              pathstack.push(child);
              if (!update || !dirExists(conf, new Path(args.dst, dst))) {
                ++dirCount;
              }
              else {
                skipPath = true; // skip creating dir at destination
              }
            }
            else {
              Path destPath = new Path(args.dst, dst);
              if (cur.isFile() && (args.srcs.size() == 1)) {
                // Copying a single file; use dst path provided by user as
                // destination file rather than destination directory
                Path dstparent = destPath.getParent();
                FileSystem destFileSys = destPath.getFileSystem(jobConf);
                if (!(destFileSys.exists(dstparent) &&
                    destFileSys.getFileStatus(dstparent).isDirectory())) {
                  destPath = dstparent;
                }
              }
              //skip path if the src and the dst files are the same.
              skipPath = update && 
              	sameFile(srcfs, child, dstfs, destPath, skipCRCCheck);
              //skip path if it exceed file limit or size limit
              skipPath |= fileCount == args.filelimit
                          || byteCount + child.getLen() > args.sizelimit; 

              if (!skipPath) {
                ++fileCount;
                byteCount += child.getLen();

                if (LOG.isTraceEnabled()) {
                  LOG.trace("adding file " + child.getPath());
                }

                ++cnsyncf;
                cbsyncs += child.getLen();
                if (cnsyncf > SYNC_FILE_MAX || cbsyncs > maxBytesPerMap) {
                  src_writer.sync();
                  dst_writer.sync();
                  cnsyncf = 0;
                  cbsyncs = 0L;
                }
              }
              else {
                ++skipFileCount;
                skipByteCount += child.getLen();
                if (LOG.isTraceEnabled()) {
                  LOG.trace("skipping file " + child.getPath());
                }
              }
            }

            if (!skipPath) {
              src_writer.append(new LongWritable(child.isDirectory()? 0: child.getLen()),
                  new FilePair(child, dst));
            }

            dst_writer.append(new Text(dst),
                new Text(child.getPath().toString()));
          }

          if (cur.isDirectory()) {
            String dst = makeRelative(root, cur.getPath());
            dir_writer.append(new Text(dst), new FilePair(cur, dst));
            if (++dirsyn > SYNC_FILE_MAX) {
              dirsyn = 0;
              dir_writer.sync();                
            }
          }
        }
      }
    }
    LOG.info("sourcePathsCount(files+directories)=" + srcCount);
    LOG.info("filesToCopyCount=" + fileCount);
    LOG.info("bytesToCopyCount=" +
             TraditionalBinaryPrefix.long2String(byteCount, "", 1));
    if (update) {
      LOG.info("filesToSkipCopyCount=" + skipFileCount);
      LOG.info("bytesToSkipCopyCount=" +
               TraditionalBinaryPrefix.long2String(skipByteCount, "", 1));
    }
    if (args.dryrun) {
      return false;
    }
    int mapCount = setMapCount(byteCount, jobConf);
    // Increase the replication of _distcp_src_files, if needed
    setReplication(conf, jobConf, srcfilelist, mapCount);
    
    FileStatus dststatus = null;
    try {
      dststatus = dstfs.getFileStatus(args.dst);
    } catch(FileNotFoundException fnfe) {
      LOG.info(args.dst + " does not exist.");
    }

    // create dest path dir if copying > 1 file
    if (dststatus == null) {
      if (srcCount > 1 && !dstfs.mkdirs(args.dst)) {
        throw new IOException("Failed to create" + args.dst);
      }
    }
    
    final Path sorted = new Path(jobDirectory, "_distcp_sorted"); 
    checkDuplication(jobfs, dstfilelist, sorted, conf);

    if (dststatus != null && args.flags.contains(Options.DELETE)) {
      long deletedPathsCount = deleteNonexisting(dstfs, dststatus, sorted,
          jobfs, jobDirectory, jobConf, conf);
      LOG.info("deletedPathsFromDestCount(files+directories)=" +
               deletedPathsCount);
    }

    Path tmpDir = new Path(
        (dstExists && !dstIsDir) || (!dstExists && srcCount == 1)?
        args.dst.getParent(): args.dst, "_distcp_tmp_" + randomId);
    jobConf.set(TMP_DIR_LABEL, tmpDir.toUri().toString());

    // Explicitly create the tmpDir to ensure that it can be cleaned
    // up by fullyDelete() later.
    tmpDir.getFileSystem(conf).mkdirs(tmpDir);

    LOG.info("sourcePathsCount=" + srcCount);
    LOG.info("filesToCopyCount=" + fileCount);
    LOG.info("bytesToCopyCount=" +
             TraditionalBinaryPrefix.long2String(byteCount, "", 1));
    jobConf.setInt(SRC_COUNT_LABEL, srcCount);
    jobConf.setLong(TOTAL_SIZE_LABEL, byteCount);
    
    return (fileCount + dirCount) > 0;
  }

  /**
   * Check whether the contents of src and dst are the same.
   * 
   * Return false if dstpath does not exist
   * 
   * If the files have different sizes, return false.
   * 
   * If the files have the same sizes, the file checksums will be compared.
   * 
   * When file checksum is not supported in any of file systems,
   * two files are considered as the same if they have the same size.
   */
  static private boolean sameFile(FileSystem srcfs, FileStatus srcstatus,
      FileSystem dstfs, Path dstpath, boolean skipCRCCheck) throws IOException {
    FileStatus dststatus;
    try {
      dststatus = dstfs.getFileStatus(dstpath);
    } catch(FileNotFoundException fnfe) {
      return false;
    }

    //same length?
    if (srcstatus.getLen() != dststatus.getLen()) {
      return false;
    }

    if (skipCRCCheck) {
      LOG.debug("Skipping the CRC check");
      return true;
    }
    
    //get src checksum
    final FileChecksum srccs;
    try {
      srccs = srcfs.getFileChecksum(srcstatus.getPath());
    } catch(FileNotFoundException fnfe) {
      /*
       * Two possible cases:
       * (1) src existed once but was deleted between the time period that
       *     srcstatus was obtained and the try block above.
       * (2) srcfs does not support file checksum and (incorrectly) throws
       *     FNFE, e.g. some previous versions of HftpFileSystem.
       * For case (1), it is okay to return true since src was already deleted.
       * For case (2), true should be returned.  
       */
      return true;
    }

    //compare checksums
    try {
      final FileChecksum dstcs = dstfs.getFileChecksum(dststatus.getPath());
      //return true if checksum is not supported
      //(i.e. some of the checksums is null)
      return srccs == null || dstcs == null || srccs.equals(dstcs);
    } catch(FileNotFoundException fnfe) {
      return false;
    }
  }
  
  /**
   * Delete the dst files/dirs which do not exist in src
   * 
   * @return total count of files and directories deleted from destination
   * @throws IOException
   */
  static private long deleteNonexisting(
      FileSystem dstfs, FileStatus dstroot, Path dstsorted,
      FileSystem jobfs, Path jobdir, JobConf jobconf, Configuration conf
      ) throws IOException {
    if (dstroot.isFile()) {
      throw new IOException("dst must be a directory when option "
          + Options.DELETE.cmd + " is set, but dst (= " + dstroot.getPath()
          + ") is not a directory.");
    }

    //write dst lsr results
    final Path dstlsr = new Path(jobdir, "_distcp_dst_lsr");
    try (final SequenceFile.Writer writer = SequenceFile.createWriter(jobconf,
        Writer.file(dstlsr), Writer.keyClass(Text.class),
        Writer.valueClass(NullWritable.class), Writer.compression(
        SequenceFile.CompressionType.NONE))) {
      //do lsr to get all file statuses in dstroot
      final Stack<FileStatus> lsrstack = new Stack<FileStatus>();
      for(lsrstack.push(dstroot); !lsrstack.isEmpty(); ) {
        final FileStatus status = lsrstack.pop();
        if (status.isDirectory()) {
          for(FileStatus child : dstfs.listStatus(status.getPath())) {
            String relative = makeRelative(dstroot.getPath(), child.getPath());
            writer.append(new Text(relative), NullWritable.get());
            lsrstack.push(child);
          }
        }
      }
    }

    //sort lsr results
    final Path sortedlsr = new Path(jobdir, "_distcp_dst_lsr_sorted");
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(jobfs,
        new Text.Comparator(), Text.class, NullWritable.class, jobconf);
    sorter.sort(dstlsr, sortedlsr);

    //compare lsr list and dst list  
    long deletedPathsCount = 0;
    try (SequenceFile.Reader lsrin =
             new SequenceFile.Reader(jobconf, Reader.file(sortedlsr));
         SequenceFile.Reader  dstin =
             new SequenceFile.Reader(jobconf, Reader.file(dstsorted))) {
      //compare sorted lsr list and sorted dst list
      final Text lsrpath = new Text();
      final Text dstpath = new Text();
      final Text dstfrom = new Text();
      final Trash trash = new Trash(dstfs, conf);
      Path lastpath = null;

      boolean hasnext = dstin.next(dstpath, dstfrom);
      while (lsrin.next(lsrpath, NullWritable.get())) {
        int dst_cmp_lsr = dstpath.compareTo(lsrpath);
        while (hasnext && dst_cmp_lsr < 0) {
          hasnext = dstin.next(dstpath, dstfrom);
          dst_cmp_lsr = dstpath.compareTo(lsrpath);
        }
        
        if (dst_cmp_lsr == 0) {
          //lsrpath exists in dst, skip it
          hasnext = dstin.next(dstpath, dstfrom);
        } else {
          //lsrpath does not exist, delete it
          final Path rmpath = new Path(dstroot.getPath(), lsrpath.toString());
          ++deletedPathsCount;
          if ((lastpath == null || !isAncestorPath(lastpath, rmpath))) {
            if (!(trash.moveToTrash(rmpath) || dstfs.delete(rmpath, true))) {
              throw new IOException("Failed to delete " + rmpath);
            }
            lastpath = rmpath;
          }
        }
      }
    }
    return deletedPathsCount;
  }

  //is x an ancestor path of y?
  static private boolean isAncestorPath(Path xp, Path yp) {
    final String x = xp.toString();
    final String y = yp.toString();
    if (!y.startsWith(x)) {
      return false;
    }
    final int len = x.length();
    return y.length() == len || y.charAt(len) == Path.SEPARATOR_CHAR;  
  }
  
  /** Check whether the file list have duplication. */
  static private void checkDuplication(FileSystem fs, Path file, Path sorted,
    Configuration conf) throws IOException {
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
      new Text.Comparator(), Text.class, Text.class, conf);
    sorter.sort(file, sorted);
    try (SequenceFile.Reader in =
         new SequenceFile.Reader(conf, Reader.file(sorted))) {
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
  } 

  /** An exception class for duplicated source files. */
  public static class DuplicationException extends IOException {
    private static final long serialVersionUID = 1L;
    /** Error code for this exception */
    public static final int ERROR_CODE = -2;
    DuplicationException(String message) {super(message);}
  }
}
