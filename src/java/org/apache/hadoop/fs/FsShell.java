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
package org.apache.hadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.dfs.DistributedFileSystem;

/** Provide command line access to a FileSystem. */
public class FsShell extends Configured implements Tool {

  protected FileSystem fs;
  private Trash trash;
  public static final SimpleDateFormat dateForm = 
    new SimpleDateFormat("yyyy-MM-dd HH:mm");
  protected static final SimpleDateFormat modifFmt =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static {
    modifFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
  static final String SETREP_SHORT_USAGE="-setrep [-R] [-w] <rep> <path/file>";
  static final String GET_SHORT_USAGE = "-get [-ignoreCrc] [-crc] <src> <localdst>";
  static final String COPYTOLOCAL_SHORT_USAGE = GET_SHORT_USAGE.replace(
      "-get", "-copyToLocal");
  static final String TAIL_USAGE="-tail [-f] <file>";
  private static final DecimalFormat decimalFormat;
  static {
	  NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
	  decimalFormat = (DecimalFormat) numberFormat;
	  decimalFormat.applyPattern("#.##");
  }

  /**
   */
  public FsShell() {
    this(null);
  }

  public FsShell(Configuration conf) {
    super(conf);
    fs = null;
    trash = null;
  }
  
  protected void init() throws IOException {
    getConf().setQuietMode(true);
    if (this.fs == null) {
     this.fs = FileSystem.get(getConf());
    }
    if (this.trash == null) {
      this.trash = new Trash(getConf());
    }
  }

  
  /**
   * Copies from stdin to the indicated file.
   */
  private void copyFromStdin(Path dst, FileSystem dstFs) throws IOException {
    if (dstFs.isDirectory(dst)) {
      throw new IOException("When source is stdin, destination must be a file.");
    }
    if (dstFs.exists(dst)) {
      throw new IOException("Target " + dst.toString() + " already exists.");
    }
    FSDataOutputStream out = dstFs.create(dst); 
    try {
      IOUtils.copyBytes(System.in, out, getConf(), false);
    } 
    finally {
      out.close();
    }
  }

  /** 
   * Print from src to stdout.
   */
  private void printToStdout(InputStream in) throws IOException {
    try {
      IOUtils.copyBytes(in, System.out, getConf(), false);
    } finally {
      in.close();
    }
  }

  /**
   * Add a local file to the indicated FileSystem name. src is kept.
   */
  void copyFromLocal(Path src, String dstf) throws IOException {
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    if (src.toString().equals("-")) {
      copyFromStdin(dstPath, dstFs);
    } else {
      dstFs.copyFromLocalFile(false, false, src, new Path(dstf));
    }
  }

  /**
   * Add a local file to the indicated FileSystem name. src is removed.
   */
  void moveFromLocal(Path src, String dstf) throws IOException {
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    dstFs.moveFromLocalFile(src, dstPath);
  }

  /**
   * Obtain the indicated files that match the file pattern <i>srcf</i>
   * and copy them to the local name. srcf is kept.
   * When copying multiple files, the destination must be a directory. 
   * Otherwise, IOException is thrown.
   * @param argv: arguments
   * @param pos: Ignore everything before argv[pos]  
   * @exception: IOException  
   * @see org.apache.hadoop.fs.FileSystem.globPaths 
   */
  void copyToLocal(String[]argv, int pos) throws IOException {
    CommandFormat cf = new CommandFormat("copyToLocal", 2,2,"crc","ignoreCrc");
    
    String srcstr = null;
    String dststr = null;
    try {
      List<String> parameters = cf.parse(argv, pos);
      srcstr = parameters.get(0);
      dststr = parameters.get(1);
    }
    catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + GET_SHORT_USAGE);
      throw iae;
    }
    final boolean copyCrc = cf.options.get("crc");
    final boolean verifyChecksum = !cf.options.get("ignoreCrc");

    if (dststr.equals("-")) {
      if (copyCrc) {
        System.err.println("-crc option is not valid when destination is stdout.");
      }
      cat(srcstr, verifyChecksum);
    } else {
      File dst = new File(dststr);      
      Path srcpath = new Path(srcstr);
      FileSystem srcFS = getSrcFileSystem(srcpath, verifyChecksum);
      FileStatus[] srcs = srcFS.globStatus(srcpath);
      boolean dstIsDir = dst.isDirectory(); 
      if (srcs.length > 1 && !dstIsDir) {
        throw new IOException("When copying multiple files, "
                              + "destination should be a directory.");
      }
      for (FileStatus status : srcs) {
        Path p = status.getPath();
        File f = dstIsDir? new File(dst, p.getName()): dst;
        copyToLocal(srcFS, p, f, copyCrc);
      }
    }
  }

  /**
   * Return the {@link FileSystem} specified by src and the conf.
   * It the {@link FileSystem} supports checksum, set verifyChecksum.
   */
  private FileSystem getSrcFileSystem(Path src, boolean verifyChecksum
      ) throws IOException { 
    FileSystem srcFs = src.getFileSystem(getConf());
    if (srcFs instanceof DistributedFileSystem) {
      ((DistributedFileSystem)srcFs).setVerifyChecksum(verifyChecksum);
    }
    return srcFs;
  }

  /**
   * The prefix for the tmp file used in copyToLocal.
   * It must be at least three characters long, required by
   * {@link java.io.File#createTempFile(String, String, File)}.
   */
  static final String COPYTOLOCAL_PREFIX = "_copyToLocal_";

  /**
   * Copy a source file from a given file system to local destination.
   * @param srcFS source file system
   * @param src source path
   * @param dst destination
   * @param copyCrc copy CRC files?
   * @exception IOException If some IO failed
   */
  private void copyToLocal(final FileSystem srcFS, final Path src,
                           final File dst, final boolean copyCrc)
    throws IOException {
    /* Keep the structure similar to ChecksumFileSystem.copyToLocal(). 
     * Ideal these two should just invoke FileUtil.copy() and not repeat
     * recursion here. Of course, copy() should support two more options :
     * copyCrc and useTmpFile (may be useTmpFile need not be an option).
     */
    
    if (!srcFS.getFileStatus(src).isDir()) {
      if (dst.exists()) {
        // match the error message in FileUtil.checkDest():
        throw new IOException("Target " + dst + " already exists");
      }
      
      // use absolute name so that tmp file is always created under dest dir
      File tmp = FileUtil.createLocalTempFile(dst.getAbsoluteFile(),
                                              COPYTOLOCAL_PREFIX, true);
      if (!FileUtil.copy(srcFS, src, tmp, false, srcFS.getConf())) {
        throw new IOException("Failed to copy " + src + " to " + dst); 
      }
      
      if (!tmp.renameTo(dst)) {
        throw new IOException("Failed to rename tmp file " + tmp + 
                              " to local destination \"" + dst + "\".");
      }

      if (copyCrc) {
        ChecksumFileSystem csfs = (ChecksumFileSystem) srcFS;
        File dstcs = FileSystem.getLocal(srcFS.getConf())
          .pathToFile(csfs.getChecksumFile(new Path(dst.getCanonicalPath())));
        copyToLocal(csfs.getRawFileSystem(), csfs.getChecksumFile(src),
                    dstcs, false);
      } 
    } else {
      // once FileUtil.copy() supports tmp file, we don't need to mkdirs().
      dst.mkdirs();
      for(Path path : srcFS.listPaths(src)) {
        copyToLocal(srcFS, path, new File(dst, path.getName()), copyCrc);
      }
    }
  }

  /**
   * Get all the files in the directories that match the source file 
   * pattern and merge and sort them to only one file on local fs 
   * srcf is kept.
   * @param srcf: a file pattern specifying source files
   * @param dstf: a destination local file/directory 
   * @exception: IOException  
   * @see org.apache.hadoop.fs.FileSystem.globPaths 
   */
  void copyMergeToLocal(String srcf, Path dst) throws IOException {
    copyMergeToLocal(srcf, dst, false);
  }    
    

  /**
   * Get all the files in the directories that match the source file pattern
   * and merge and sort them to only one file on local fs 
   * srcf is kept.
   * 
   * Also adds a string between the files (useful for adding \n
   * to a text file)
   * @param srcf: a file pattern specifying source files
   * @param dstf: a destination local file/directory
   * @param endline: if an end of line character is added to a text file 
   * @exception: IOException  
   * @see org.apache.hadoop.fs.FileSystem.globPaths 
   */
  void copyMergeToLocal(String srcf, Path dst, boolean endline) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path [] srcs = srcFs.globPaths(new Path(srcf));
    for(int i=0; i<srcs.length; i++) {
      if (endline) {
        FileUtil.copyMerge(srcFs, srcs[i], 
                           FileSystem.getLocal(getConf()), dst, false, getConf(), "\n");
      } else {
        FileUtil.copyMerge(srcFs, srcs[i], 
                           FileSystem.getLocal(getConf()), dst, false, getConf(), null);
      }
    }
  }      

  /**
   * Obtain the indicated file and copy to the local name.
   * srcf is removed.
   */
  void moveToLocal(String srcf, Path dst) throws IOException {
    System.err.println("Option '-moveToLocal' is not implemented yet.");
  }

  /**
   * Fetch all files that match the file pattern <i>srcf</i> and display
   * their content on stdout. 
   * @param srcf: a file pattern specifying source files
   * @exception: IOException
   * @see org.apache.hadoop.fs.FileSystem.globPaths 
   */
  void cat(String src, boolean verifyChecksum) throws IOException {
    //cat behavior in Linux
    //  [~/1207]$ ls ?.txt
    //  x.txt  z.txt
    //  [~/1207]$ cat x.txt y.txt z.txt
    //  xxx
    //  cat: y.txt: No such file or directory
    //  zzz

    Path srcPattern = new Path(src);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        if (srcFs.getFileStatus(p).isDir()) {
          throw new IOException("Source must be a file.");
        }
        printToStdout(srcFs.open(p));
      }
    }.process(srcPattern, getSrcFileSystem(srcPattern, verifyChecksum));
  }

  private class TextRecordInputStream extends InputStream {
    SequenceFile.Reader r;
    WritableComparable key;
    Writable val;

    DataInputBuffer inbuf;
    DataOutputBuffer outbuf;

    public TextRecordInputStream(FileStatus f) throws IOException {
      r = new SequenceFile.Reader(fs, f.getPath(), getConf());
      key = (WritableComparable)ReflectionUtils.newInstance(
          r.getKeyClass(), getConf());
      val = (Writable)ReflectionUtils.newInstance(
          r.getValueClass(), getConf());
      inbuf = new DataInputBuffer();
      outbuf = new DataOutputBuffer();
    }

    public int read() throws IOException {
      int ret;
      if (null == inbuf || -1 == (ret = inbuf.read())) {
        if (!r.next(key, val)) {
          return -1;
        }
        byte[] tmp = key.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\t');
        tmp = val.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\n');
        inbuf.reset(outbuf.getData(), outbuf.getLength());
        outbuf.reset();
        ret = inbuf.read();
      }
      return ret;
    }
  }

  private InputStream forMagic(Path p, FileSystem srcFs) throws IOException {
    FSDataInputStream i = srcFs.open(p);
    switch(i.readShort()) {
      case 0x1f8b: // RFC 1952
        i.seek(0);
        return new GZIPInputStream(i);
      case 0x5345: // 'S' 'E'
        if (i.readByte() == 'Q') {
          i.close();
          return new TextRecordInputStream(srcFs.getFileStatus(p));
        }
        break;
    }
    i.seek(0);
    return i;
  }

  void text(String srcf) throws IOException {
    Path srcPattern = new Path(srcf);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        if (srcFs.isDirectory(p)) {
          throw new IOException("Source must be a file.");
        }
        printToStdout(forMagic(p, srcFs));
      }
    }.process(srcPattern, srcPattern.getFileSystem(getConf()));
  }

  /**
   * Parse the args of a command and check the format of args.
   */
  static class CommandFormat {
    final String name;
    final int minPar, maxPar;
    final Map<String, Boolean> options = new HashMap<String, Boolean>();

    private CommandFormat(String n, int min, int max, String ... possibleOpt) {
      name = n;
      minPar = min;
      maxPar = max;
      for(String opt : possibleOpt)
        options.put(opt, Boolean.FALSE);
    }

    List<String> parse(String[] args, int pos) {
      List<String> parameters = new ArrayList<String>();
      for(; pos < args.length; pos++) {
        if (args[pos].charAt(0) == '-') {
          String opt = args[pos].substring(1);
          if (options.containsKey(opt))
            options.put(opt, Boolean.TRUE);
          else
            throw new IllegalArgumentException("Illegal option " + args[pos]);
        }
        else
          parameters.add(args[pos]);
      }
      int psize = parameters.size();
      if (psize < minPar || psize > maxPar)
        throw new IllegalArgumentException("Illegal number of arguments");
      return parameters;
    }
  }

  /**
   * Parse the incoming command string
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException 
   */
  private void setReplication(String[] cmd, int pos) throws IOException {
    CommandFormat c = new CommandFormat("setrep", 2, 2, "R", "w");
    String dst = null;
    short rep = 0;

    try {
      List<String> parameters = c.parse(cmd, pos);
      rep = Short.parseShort(parameters.get(0));
      dst = parameters.get(1);
    } catch (NumberFormatException nfe) {
      System.err.println("Illegal replication, a positive integer expected");
      throw nfe;
    }
    catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + SETREP_SHORT_USAGE);
      throw iae;
    }

    if (rep < 1) {
      System.err.println("Cannot set replication to: " + rep);
      throw new IllegalArgumentException("replication must be >= 1");
    }

    List<Path> waitList = c.options.get("w")? new ArrayList<Path>(): null;
    setReplication(rep, dst, c.options.get("R"), waitList);

    if (waitList != null) {
      waitForReplication(waitList, rep);
    }
  }
    
  /**
   * Wait for all files in waitList to have replication number equal to rep.
   * @param waitList The files are waited for.
   * @param rep The new replication number.
   * @throws IOException IOException
   */
  void waitForReplication(List<Path> waitList, int rep) throws IOException {
    for(Path f : waitList) {
      System.out.print("Waiting for " + f + " ...");
      System.out.flush();

      boolean printWarning = false;
      long len = fs.getFileStatus(f).getLen();

      for(boolean done = false; !done; ) {
        String[][] locations = fs.getFileCacheHints(f, 0, len);
        int i = 0;
        for(; i < locations.length && locations[i].length == rep; i++)
          if (!printWarning && locations[i].length > rep) {
            System.out.println("\nWARNING: the waiting time may be long for "
                + "DECREASING the number of replication.");
            printWarning = true;
          }
        done = i == locations.length;

        if (!done) {
          System.out.print(".");
          System.out.flush();
          try {Thread.sleep(10000);} catch (InterruptedException e) {}
        }
      }

      System.out.println(" done");
    }
  }

  /**
   * Set the replication for files that match file pattern <i>srcf</i>
   * if it's a directory and recursive is true,
   * set replication for all the subdirs and those files too.
   * @param newRep new replication factor
   * @param srcf a file pattern specifying source files
   * @param recursive if need to set replication factor for files in subdirs
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
   */
  void setReplication(short newRep, String srcf, boolean recursive,
                      List<Path> waitingList)
    throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path[] srcs = srcFs.globPaths(new Path(srcf));
    for(int i=0; i<srcs.length; i++) {
      setReplication(newRep, srcFs, srcs[i], recursive, waitingList);
    }
  }

  private void setReplication(short newRep, FileSystem srcFs, 
                              Path src, boolean recursive,
                              List<Path> waitingList)
    throws IOException {
    if (!srcFs.getFileStatus(src).isDir()) {
      setFileReplication(src, srcFs, newRep, waitingList);
      return;
    }
    Path items[] = srcFs.listPaths(src);
    if (items == null) {
      throw new IOException("Could not get listing for " + src);
    } else {

      for (int i = 0; i < items.length; i++) {
        Path cur = items[i];
        if (!srcFs.getFileStatus(cur).isDir()) {
          setFileReplication(cur, srcFs, newRep, waitingList);
        } else if (recursive) {
          setReplication(newRep, srcFs, cur, recursive, waitingList);
        }
      }
    }
  }
    
  /**
   * Actually set the replication for this file
   * If it fails either throw IOException or print an error msg
   * @param file: a file/directory
   * @param newRep: new replication factor
   * @throws IOException
   */
  private void setFileReplication(Path file, FileSystem srcFs, short newRep, List<Path> waitList)
    throws IOException {
    if (srcFs.setReplication(file, newRep)) {
      if (waitList != null) {
        waitList.add(file);
      }
      System.out.println("Replication " + newRep + " set: " + file);
    } else {
      System.err.println("Could not set replication for: " + file);
    }
  }
    
    
  /**
   * Get a listing of all files in that match the file pattern <i>srcf</i>.
   * @param srcf a file pattern specifying source files
   * @param recursive if need to list files in subdirs
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
   */
  void ls(String srcf, boolean recursive) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(this.getConf());
    FileStatus[] srcs = srcFs.globStatus(srcPath);
    if (srcs==null || srcs.length==0) {
      throw new FileNotFoundException("Cannot access " + srcf + 
          ": No such file or directory.");
    }
 
    boolean printHeader = (srcs.length == 1) ? true: false;
    for(int i=0; i<srcs.length; i++) {
      ls(srcs[i].getPath(), srcFs, recursive, printHeader);
    }
  }

  /* list all files under the directory <i>src</i>
   * ideally we should provide "-l" option, that lists like "ls -l".
   */
  private void ls(Path src, FileSystem srcFs, boolean recursive, boolean printHeader) throws IOException {
    FileStatus items[] = srcFs.listStatus(src);
    if ((items == null) || ((items.length == 0) 
        && (!srcFs.exists(src)))) {
      throw new FileNotFoundException(src + ": No such file or directory.");
    } else {
      if (!recursive && printHeader) {
        if (items.length != 0) {
          System.out.println("Found " + items.length + " items");
        }
      }
      for (int i = 0; i < items.length; i++) {
        FileStatus stat = items[i];
        Path cur = stat.getPath();
        String mdate = dateForm.format(new Date(stat.getModificationTime()));
        System.out.println(cur.toUri().getPath() + "\t" 
                           + (stat.isDir() ? 
                              "<dir>\t" : 
                              ("<r " + stat.getReplication() 
                               + ">\t" + stat.getLen()))
                           + "\t" + mdate 
                           + "\t" + stat.getPermission()
                           + "\t" + stat.getOwner() 
                           + "\t" + stat.getGroup());
        if (recursive && stat.isDir()) {
          ls(cur,srcFs, recursive, printHeader);
        }
      }
    }
  }

  /**
   * Show the size of all files that match the file pattern <i>src</i>
   * @param src a file pattern specifying source files
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
   */
  void du(String src) throws IOException {
    Path srcPath = new Path(src);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path items[] = srcFs.listPaths(srcFs.globPaths(srcPath));
    if ((items == null) || ((items.length == 0) && 
        (!srcFs.exists(srcPath)))){
      throw new FileNotFoundException("Cannot access " + src
            + ": No such file or directory.");
    } else {
      System.out.println("Found " + items.length + " items");
      for (int i = 0; i < items.length; i++) {
        Path cur = items[i];
        System.out.println(cur + "\t" + srcFs.getContentLength(cur));
      }
    }
  }
    
  /**
   * Show the summary disk usage of each dir/file 
   * that matches the file pattern <i>src</i>
   * @param src a file pattern specifying source files
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
   */
  void dus(String src) throws IOException {
    Path srcPath = new Path(src);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    FileStatus status[] = srcFs.globStatus(new Path(src));
    if (status==null || status.length==0) {
      throw new FileNotFoundException("Cannot access " + src + 
          ": No such file or directory.");
    }
    for(int i=0; i<status.length; i++) {
      FileStatus items[] = srcFs.listStatus(status[i].getPath());
      if (items != null) {
        long totalSize=0;
        for(int j=0; j<items.length; j++) {
          totalSize += srcFs.getContentLength(
              items[j].getPath());
        }
        String pathStr = status[i].getPath().toString();
        System.out.println(
                           ("".equals(pathStr)?".":pathStr) + "\t" + totalSize);
      }
    }
  }

  /**
   * Create the given dir
   */
  void mkdir(String src) throws IOException {
    Path f = new Path(src);
    FileSystem srcFs = f.getFileSystem(getConf());
    FileStatus fstatus = null;
    try {
      fstatus = srcFs.getFileStatus(f);
      if (fstatus.isDir()) {
        throw new IOException("cannot create directory " 
            + src + ": File exists");
      }
      else {
        throw new IOException(src + " exists but " +
            "is not a directory");
      }
    } catch(FileNotFoundException e) {
        if (!srcFs.mkdirs(f)) {
          throw new IOException("failed to create " + src);
        }
    }
  }

  /**
   * (Re)create zero-length file at the specified path.
   * This will be replaced by a more UNIX-like touch when files may be
   * modified.
   */
  void touchz(String src) throws IOException {
    Path f = new Path(src);
    FileSystem srcFs = f.getFileSystem(getConf());
    FileStatus st;
    if (srcFs.exists(f)) {
      st = srcFs.getFileStatus(f);
      if (st.isDir()) {
        // TODO: handle this
        throw new IOException(src + " is a directory");
      } else if (st.getLen() != 0)
        throw new IOException(src + " must be a zero-length file");
    }
    FSDataOutputStream out = srcFs.create(f);
    out.close();
  }

  /**
   * Check file types.
   */
  int test(String argv[], int i) throws IOException {
    if (!argv[i].startsWith("-") || argv[i].length() > 2)
      throw new IOException("Not a flag: " + argv[i]);
    char flag = argv[i].toCharArray()[1];
    Path f = new Path(argv[++i]);
    FileSystem srcFs = f.getFileSystem(getConf());
    switch(flag) {
      case 'e':
        return srcFs.exists(f) ? 1 : 0;
      case 'z':
        return srcFs.getFileStatus(f).getLen() == 0 ? 1 : 0;
      case 'd':
        return srcFs.getFileStatus(f).isDir() ? 1 : 0;
      default:
        throw new IOException("Unknown flag: " + flag);
    }
  }

  /**
   * Print statistics about path in specified format.
   * Format sequences:
   *   %b: Size of file in blocks
   *   %n: Filename
   *   %o: Block size
   *   %r: replication
   *   %y: UTC date as &quot;yyyy-MM-dd HH:mm:ss&quot;
   *   %Y: Milliseconds since January 1, 1970 UTC
   */
  void stat(char[] fmt, String src) throws IOException {
    Path srcPath = new Path(src);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path glob[] = srcFs.globPaths(srcPath);
    if (null == glob)
      throw new IOException("cannot stat `" + src + "': No such file or directory");
    for (Path f : glob) {
      FileStatus st = srcFs.getFileStatus(f);
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < fmt.length; ++i) {
        if (fmt[i] != '%') {
          buf.append(fmt[i]);
        } else {
          if (i + 1 == fmt.length) break;
          switch(fmt[++i]) {
            case 'b':
              buf.append(st.getLen());
              break;
            case 'F':
              buf.append(st.isDir() ? "directory" : "regular file");
              break;
            case 'n':
              buf.append(f.getName());
              break;
            case 'o':
              buf.append(st.getBlockSize());
              break;
            case 'r':
              buf.append(st.getReplication());
              break;
            case 'y':
              buf.append(modifFmt.format(new Date(st.getModificationTime())));
              break;
            case 'Y':
              buf.append(st.getModificationTime());
              break;
            default:
              buf.append(fmt[i]);
              break;
          }
        }
      }
      System.out.println(buf.toString());
    }
  }

  /**
   * Move files that match the file pattern <i>srcf</i>
   * to a destination file.
   * When moving mutiple files, the destination must be a directory. 
   * Otherwise, IOException is thrown.
   * @param srcf a file pattern specifying source files
   * @param dstf a destination local file/directory 
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
   */
  void rename(String srcf, String dstf) throws IOException {
    Path srcPath = new Path(srcf);
    Path dstPath = new Path(dstf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    URI srcURI = srcFs.getUri();
    URI dstURI = dstFs.getUri();
    if (srcURI.compareTo(dstURI) != 0) {
      throw new IOException("src and destination filesystems do not match.");
    }
    Path [] srcs = srcFs.globPaths(new Path(srcf));
    Path dst = new Path(dstf);
    if (srcs.length > 1 && !srcFs.isDirectory(dst)) {
      throw new IOException("When moving multiple files, " 
                            + "destination should be a directory.");
    }
    for(int i=0; i<srcs.length; i++) {
      if (!srcFs.rename(srcs[i], dst)) {
        FileStatus srcFstatus = null;
        FileStatus dstFstatus = null;
        try {
          srcFstatus = srcFs.getFileStatus(srcs[i]);
        } catch(FileNotFoundException e) {
          throw new FileNotFoundException(srcs[i] + 
          ": No such file or directory");
        }
        try {
          dstFstatus = dstFs.getFileStatus(dst);
        } catch(IOException e) {
          //hide this
        }
        if((srcFstatus!= null) && (dstFstatus!= null)) {
          if (srcFstatus.isDir()  && !dstFstatus.isDir()) {
            throw new IOException("cannot overwrite non directory "
                + dst + " with directory " + srcs[i]);
          }
        }
      }
    }
  }

  /**
   * Move/rename file(s) to a destination file. Multiple source
   * files can be specified. The destination is the last element of
   * the argvp[] array.
   * If multiple source files are specified, then the destination 
   * must be a directory. Otherwise, IOException is thrown.
   * @exception: IOException  
   */
  private int rename(String argv[], Configuration conf) throws IOException {
    int i = 0;
    int exitCode = 0;
    String cmd = argv[i++];  
    String dest = argv[argv.length-1];
    //
    // If the user has specified multiple source files, then
    // the destination has to be a directory
    //
    if (argv.length > 3) {
      Path dst = new Path(dest);
      FileSystem dstFs = dst.getFileSystem(getConf());
      if (!dstFs.isDirectory(dst)) {
        throw new IOException("When moving multiple files, " 
                              + "destination " + dest + " should be a directory.");
      }
    }
    //
    // for each source file, issue the rename
    //
    for (; i < argv.length - 1; i++) {
      try {
        //
        // issue the rename to the fs
        //
        rename(argv[i], dest);
      } catch (RemoteException e) {
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error mesage.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " +
                             content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        //
        // IO exception encountered locally.
        //
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": " +
                           e.getLocalizedMessage());
      }
    }
    return exitCode;
  }

  /**
   * Copy files that match the file pattern <i>srcf</i>
   * to a destination file.
   * When copying mutiple files, the destination must be a directory. 
   * Otherwise, IOException is thrown.
   * @param srcf a file pattern specifying source files
   * @param dstf a destination local file/directory 
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
   */
  void copy(String srcf, String dstf, Configuration conf) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    Path [] srcs = srcFs.globPaths(srcPath);
    if (srcs.length > 1 && !dstFs.isDirectory(dstPath)) {
      throw new IOException("When copying multiple files, " 
                            + "destination should be a directory.");
    }
    for(int i=0; i<srcs.length; i++) {
      FileUtil.copy(srcFs, srcs[i], dstFs, dstPath, false, conf);
    }
  }

  /**
   * Copy file(s) to a destination file. Multiple source
   * files can be specified. The destination is the last element of
   * the argvp[] array.
   * If multiple source files are specified, then the destination 
   * must be a directory. Otherwise, IOException is thrown.
   * @exception: IOException  
   */
  private int copy(String argv[], Configuration conf) throws IOException {
    int i = 0;
    int exitCode = 0;
    String cmd = argv[i++];  
    String dest = argv[argv.length-1];
    //
    // If the user has specified multiple source files, then
    // the destination has to be a directory
    //
    if (argv.length > 3) {
      Path dst = new Path(dest);
      FileSystem dstFs = dst.getFileSystem(getConf());
      if (!fs.isDirectory(dst)) {
        throw new IOException("When copying multiple files, " 
                              + "destination " + dest + " should be a directory.");
      }
    }
    //
    // for each source file, issue the copy
    //
    for (; i < argv.length - 1; i++) {
      try {
        //
        // issue the copy to the fs
        //
        copy(argv[i], dest, conf);
      } catch (RemoteException e) {
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error mesage.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " +
                             content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        //
        // IO exception encountered locally.
        //
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": " +
                           e.getLocalizedMessage());
      }
    }
    return exitCode;
  }

  /**
   * Delete all files that match the file pattern <i>srcf</i>.
   * @param srcf a file pattern specifying source files
   * @param recursive if need to delete subdirs
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globPaths(Path)
   */
  void delete(String srcf, final boolean recursive) throws IOException {
    //rm behavior in Linux
    //  [~/1207]$ ls ?.txt
    //  x.txt  z.txt
    //  [~/1207]$ rm x.txt y.txt z.txt 
    //  rm: cannot remove `y.txt': No such file or directory

    Path srcPattern = new Path(srcf);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        delete(p, srcFs, recursive);
      }
    }.process(srcPattern, srcPattern.getFileSystem(getConf()));
  }
    
  /* delete a file */
  private void delete(Path src, FileSystem srcFs, boolean recursive) throws IOException {
    if (srcFs.isDirectory(src) && !recursive) {
      throw new IOException("Cannot remove directory \"" + src +
                            "\", use -rmr instead");
    }
    Trash trashTmp = new Trash(srcFs.getConf());
    if (trashTmp.moveToTrash(src)) {
      System.out.println("Moved to trash: " + src);
      return;
    }
    if (srcFs.delete(src)) {
      System.out.println("Deleted " + src);
    } else {
      if (!srcFs.exists(src)) {
        throw new FileNotFoundException("cannot remove "
            + src + ": No such file or directory.");
        }
      throw new IOException("Delete failed " + src);
    }
  }

  private void expunge() throws IOException {
    trash.expunge();
    trash.checkpoint();
  }

  /**
   * Returns the Trash object associated with this shell.
   */
  public Path getCurrentTrashDir() {
    return trash.getCurrentTrashDir();
  }

  /**
   * Parse the incoming command string
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException 
   */
  private void tail(String[] cmd, int pos) throws IOException {
    CommandFormat c = new CommandFormat("tail", 1, 1, "f");
    String src = null;
    Path path = null;
    short rep = 0;

    try {
      List<String> parameters = c.parse(cmd, pos);
      src = parameters.get(0);
    } catch(IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + TAIL_USAGE);
      throw iae;
    }
    boolean foption = c.options.get("f") ? true: false;
    path = new Path(src);
    FileSystem srcFs = path.getFileSystem(getConf());
    if (srcFs.isDirectory(path)) {
      throw new IOException("Source must be a file.");
    }

    long fileSize = srcFs.getFileStatus(path).getLen();
    long offset = (fileSize > 1024) ? fileSize - 1024: 0;

    while (true) {
      FSDataInputStream in = srcFs.open(path);
      in.seek(offset);
      IOUtils.copyBytes(in, System.out, 1024, false);
      offset = in.getPos();
      in.close();
      if (!foption) {
        break;
      }
      fileSize = srcFs.getFileStatus(path).getLen();
      offset = (fileSize > offset) ? offset: fileSize;
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  /**
   * This class runs a command on a given FileStatus. This can be used for
   * running various commands like chmod, chown etc.
   */
  static abstract class CmdHandler {
    
    protected int errorCode = 0;
    protected boolean okToContinue = true;
    protected String cmdName;
    
    int getErrorCode() { return errorCode; }
    boolean okToContinue() { return okToContinue; }
    String getName() { return cmdName; }
    
    protected CmdHandler(String cmdName, FileSystem fs) {
      this.cmdName = cmdName;
    }
    
    public abstract void run(FileStatus file, FileSystem fs) throws IOException;
  }
  
  ///helper for runCmdHandler*() returns listPaths()
  private static FileStatus[] cmdHandlerListStatus(CmdHandler handler, 
                                                   FileSystem srcFs,
                                                   Path path) {
    try {
      FileStatus[] files = srcFs.listStatus(path);
      if ( files == null ) {
        System.err.println(handler.getName() + 
                           ": could not get listing for '" + path + "'");
      }
      return files;
    } catch (IOException e) {
      System.err.println(handler.getName() + 
                         ": could not get get listing for '" + path + "' : " +
                         e.getMessage().split("\n")[0]);
    }
    return null;
  }
  
  
  /**
   * Runs the command on a given file with the command handler. 
   * If recursive is set, command is run recursively.
   */                                       
  private static int runCmdHandler(CmdHandler handler, FileStatus stat, 
                                   FileSystem srcFs, 
                                   boolean recursive) throws IOException {
    int errors = 0;
    handler.run(stat, srcFs);
    if (recursive && stat.isDir() && handler.okToContinue()) {
      FileStatus[] files = cmdHandlerListStatus(handler, srcFs, 
                                                stat.getPath());
      if (files == null) {
        return 1;
      }
      for(FileStatus file : files ) {
        errors += runCmdHandler(handler, file, srcFs, recursive);
      }
    }
    return errors;
  }

  ///top level runCmdHandler
  int runCmdHandler(CmdHandler handler, String[] args,
                                   int startIndex, boolean recursive) 
                                   throws IOException {
    int errors = 0;
    
    for (int i=startIndex; i<args.length; i++) {
      Path srcPath = new Path(args[i]);
      FileSystem srcFs = srcPath.getFileSystem(getConf());
      Path[] paths = srcFs.globPaths(new Path(args[i]));
      for(Path path : paths) {
        try {
          FileStatus file = srcFs.getFileStatus(path);
          if (file == null) {
            System.err.println(handler.getName() + 
                               ": could not get status for '" + path + "'");
            errors++;
          } else {
            errors += runCmdHandler(handler, file, srcFs, recursive);
          }
        } catch (IOException e) {
          System.err.println(handler.getName() + 
                             ": could not get status for '" + path + "': " +
                             e.getMessage().split("\n")[0]);        
        }
      }
    }
    
    return (errors > 0 || handler.getErrorCode() != 0) ? 1 : 0;
  }
  
  /**
   * Return an abbreviated English-language desc of the byte length
   */
  public static String byteDesc(long len) {
    double val = 0.0;
    String ending = "";
    if (len < 1024 * 1024) {
      val = (1.0 * len) / 1024;
      ending = " KB";
    } else if (len < 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024 * 1024);
      ending = " MB";
    } else if (len < 1024L * 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024 * 1024 * 1024);
      ending = " GB";
    } else if (len < 1024L * 1024 * 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024L * 1024 * 1024 * 1024);
      ending = " TB";
    } else {
      val = (1.0 * len) / (1024L * 1024 * 1024 * 1024 * 1024);
      ending = " PB";
    }
    return limitDecimalTo2(val) + ending;
  }

  public static synchronized String limitDecimalTo2(double d) {
    return decimalFormat.format(d);
  }

  private void printHelp(String cmd) {
    String summary = "hadoop fs is the command to execute fs commands. " +
      "The full syntax is: \n\n" +
      "hadoop fs [-fs <local | file system URI>] [-conf <configuration file>]\n\t" +
      "[-D <property=value>] [-ls <path>] [-lsr <path>] [-du <path>]\n\t" + 
      "[-dus <path>] [-mv <src> <dst>] [-cp <src> <dst>] [-rm <src>]\n\t" + 
      "[-rmr <src>] [-put <localsrc> <dst>] [-copyFromLocal <localsrc> <dst>]\n\t" +
      "[-moveFromLocal <localsrc> <dst>] [" + GET_SHORT_USAGE + "\n\t" +
      "[-getmerge <src> <localdst> [addnl]] [-cat <src>]\n\t" +
      "[" + COPYTOLOCAL_SHORT_USAGE + "] [-moveToLocal <src> <localdst>]\n\t" +
      "[-mkdir <path>] [-report] [" + SETREP_SHORT_USAGE + "]\n\t" +
      "[-touchz <path>] [-test -[ezd] <path>] [-stat [format] <path>]\n\t" +
      "[-tail [-f] <path>] [-text <path>]\n\t" +
      "[" + FsShellPermissions.CHMOD_USAGE + "]\n\t" +
      "[" + FsShellPermissions.CHOWN_USAGE + "]\n\t" +
      "[" + FsShellPermissions.CHGRP_USAGE + "]\n\t" +      
      "[-help [cmd]]\n";

    String conf ="-conf <configuration file>:  Specify an application configuration file.";
 
    String D = "-D <property=value>:  Use value for given property.";
  
    String fs = "-fs [local | <file system URI>]: \tSpecify the file system to use.\n" + 
      "\t\tIf not specified, the current configuration is used, \n" +
      "\t\ttaken from the following, in increasing precedence: \n" + 
      "\t\t\thadoop-default.xml inside the hadoop jar file \n" +
      "\t\t\thadoop-default.xml in $HADOOP_CONF_DIR \n" +
      "\t\t\thadoop-site.xml in $HADOOP_CONF_DIR \n" +
      "\t\t'local' means use the local file system as your DFS. \n" +
      "\t\t<file system URI> specifies a particular file system to \n" +
      "\t\tcontact. This argument is optional but if used must appear\n" +
      "\t\tappear first on the command line.  Exactly one additional\n" +
      "\t\targument must be specified. \n";

        
    String ls = "-ls <path>: \tList the contents that match the specified file pattern. If\n" + 
      "\t\tpath is not specified, the contents of /user/<currentUser>\n" +
      "\t\twill be listed. Directory entries are of the form \n" +
      "\t\t\tdirName (full path) <dir> \n" +
      "\t\tand file entries are of the form \n" + 
      "\t\t\tfileName(full path) <r n> size \n" +
      "\t\twhere n is the number of replicas specified for the file \n" + 
      "\t\tand size is the size of the file, in bytes.\n";

    String lsr = "-lsr <path>: \tRecursively list the contents that match the specified\n" +
      "\t\tfile pattern.  Behaves very similarly to hadoop fs -ls,\n" + 
      "\t\texcept that the data is shown for all the entries in the\n" +
      "\t\tsubtree.\n";

    String du = "-du <path>: \tShow the amount of space, in bytes, used by the files that \n" +
      "\t\tmatch the specified file pattern.  Equivalent to the unix\n" + 
      "\t\tcommand \"du -sb <path>/*\" in case of a directory, \n" +
      "\t\tand to \"du -b <path>\" in case of a file.\n" +
      "\t\tThe output is in the form \n" + 
      "\t\t\tname(full path) size (in bytes)\n"; 

    String dus = "-dus <path>: \tShow the amount of space, in bytes, used by the files that \n" +
      "\t\tmatch the specified file pattern.  Equivalent to the unix\n" + 
      "\t\tcommand \"du -sb\"  The output is in the form \n" + 
      "\t\t\tname(full path) size (in bytes)\n"; 
    
    String mv = "-mv <src> <dst>:   Move files that match the specified file pattern <src>\n" +
      "\t\tto a destination <dst>.  When moving multiple files, the \n" +
      "\t\tdestination must be a directory. \n";

    String cp = "-cp <src> <dst>:   Copy files that match the file pattern <src> to a \n" +
      "\t\tdestination.  When copying multiple files, the destination\n" +
      "\t\tmust be a directory. \n";

    String rm = "-rm <src>: \tDelete all files that match the specified file pattern.\n" +
      "\t\tEquivlent to the Unix command \"rm <src>\"\n";

    String rmr = "-rmr <src>: \tRemove all directories which match the specified file \n" +
      "\t\tpattern. Equivlent to the Unix command \"rm -rf <src>\"\n";

    String put = "-put <localsrc> <dst>: \tCopy a single file from the local file system \n" +
      "\t\tinto fs. \n";

    String copyFromLocal = "-copyFromLocal <localsrc> <dst>:  Identical to the -put command.\n";

    String moveFromLocal = "-moveFromLocal <localsrc> <dst>:  Same as -put, except that the source is\n" +
      "\t\tdeleted after it's copied.\n"; 

    String get = GET_SHORT_USAGE
      + ":  Copy files that match the file pattern <src> \n" +
      "\t\tto the local name.  <src> is kept.  When copying mutiple, \n" +
      "\t\tfiles, the destination must be a directory. \n";

    String getmerge = "-getmerge <src> <localdst>:  Get all the files in the directories that \n" +
      "\t\tmatch the source file pattern and merge and sort them to only\n" +
      "\t\tone file on local fs. <src> is kept.\n";

    String cat = "-cat <src>: \tFetch all files that match the file pattern <src> \n" +
      "\t\tand display their content on stdout.\n";

    String text = "-text <path>: Attempt to decode contents if the first few bytes\n" +
      "\t\tmatch a magic number associated with a known format\n" +
      "\t\t(gzip, SequenceFile)\n";
        
    String copyToLocal = COPYTOLOCAL_SHORT_USAGE
                         + ":  Identical to the -get command.\n";

    String moveToLocal = "-moveToLocal <src> <localdst>:  Not implemented yet \n";
        
    String mkdir = "-mkdir <path>: \tCreate a directory in specified location. \n";

    String setrep = SETREP_SHORT_USAGE
      + ":  Set the replication level of a file. \n"
      + "\t\tThe -R flag requests a recursive change of replication level \n"
      + "\t\tfor an entire tree.\n";

    String touchz = "-touchz <path>: Write a timestamp in yyyy-MM-dd HH:mm:ss format\n" +
      "\t\tin a file at <path>. An error is returned if the file exists with non-zero length\n";

    String test = "-test -[ezd] <path>: If file { exists, has zero length, is a directory\n" +
      "\t\tthen return 1, else return 0.\n";

    String stat = "-stat [format] <path>: Print statistics about the file/directory at <path>\n" +
      "\t\tin the specified format. Format accepts filesize in blocks (%b), filename (%n),\n" +
      "\t\tblock size (%o), replication (%r), modification date (%y, %Y)\n";

    String tail = TAIL_USAGE
      + ":  Show the last 1KB of the file. \n"
      + "\t\tThe -f option shows apended data as the file grows. \n";

    String chmod = FsShellPermissions.CHMOD_USAGE + "\n" +
      "\t\tChanges permissions of a file.\n" +
      "\t\tThis works similar to shell's chmod with a few exceptions.\n\n" +
      "\t-R\tmodifies the files recursively. This is the only option\n" +
      "\t\tcurrently supported.\n\n" +
      "\tMODE\tMode is same as mode used for chmod shell command.\n" +
      "\t\tOnly letters recognized are 'rwxX'. E.g.: a+r,g-w,+rwx,o=r\n\n" +
      "\tOCTALMODE Mode specifed in 3 digits. Unlike shell command,\n" +
      "\t\tthis requires all three digits.\n" +
      "\t\tE.g.: 754 is same as u=rwx,g=rw,o=r\n\n" +
      "\t\tIf none of 'augo' is specified, 'a' is assumed and unlike\n" +
      "\t\tshell command, no umask is applied\n";
    
    String chown = FsShellPermissions.CHOWN_USAGE + "\n" +
      "\t\tChanges owner and group of a file.\n" +
      "\t\tThis is similar to shell's chown with a few exceptions.\n\n" +
      "\t-R\tmodifies the files recursively. This is the only option\n" +
      "\t\tcurrently supported.\n\n" +
      "\t\tIf only owner or group is specified then only owner or\n" +
      "\t\tgroup is modified. The owner and group names can only\n" + 
      "\t\tcontain digits and alphabet. These names are case sensitive.\n";
    
    String chgrp = FsShellPermissions.CHGRP_USAGE + "\n" +
      "\t\tThis is equivalent to -chown ... :GROUP ...\n";
    
    String help = "-help [cmd]: \tDisplays help for given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("fs".equals(cmd)) {
      System.out.println(fs);
    } else if ("conf".equals(cmd)) {
      System.out.println(conf);
    } else if ("D".equals(cmd)) {
      System.out.println(D);
    } else if ("ls".equals(cmd)) {
      System.out.println(ls);
    } else if ("lsr".equals(cmd)) {
      System.out.println(lsr);
    } else if ("du".equals(cmd)) {
      System.out.println(du);
    } else if ("dus".equals(cmd)) {
      System.out.println(dus);
    } else if ("rm".equals(cmd)) {
      System.out.println(rm);
    } else if ("rmr".equals(cmd)) {
      System.out.println(rmr);
    } else if ("mkdir".equals(cmd)) {
      System.out.println(mkdir);
    } else if ("mv".equals(cmd)) {
      System.out.println(mv);
    } else if ("cp".equals(cmd)) {
      System.out.println(cp);
    } else if ("put".equals(cmd)) {
      System.out.println(put);
    } else if ("copyFromLocal".equals(cmd)) {
      System.out.println(copyFromLocal);
    } else if ("moveFromLocal".equals(cmd)) {
      System.out.println(moveFromLocal);
    } else if ("get".equals(cmd)) {
      System.out.println(get);
    } else if ("getmerge".equals(cmd)) {
      System.out.println(getmerge);
    } else if ("copyToLocal".equals(cmd)) {
      System.out.println(copyToLocal);
    } else if ("moveToLocal".equals(cmd)) {
      System.out.println(moveToLocal);
    } else if ("cat".equals(cmd)) {
      System.out.println(cat);
    } else if ("get".equals(cmd)) {
      System.out.println(get);
    } else if ("setrep".equals(cmd)) {
      System.out.println(setrep);
    } else if ("touchz".equals(cmd)) {
      System.out.println(touchz);
    } else if ("test".equals(cmd)) {
      System.out.println(test);
    } else if ("stat".equals(cmd)) {
      System.out.println(stat);
    } else if ("tail".equals(cmd)) {
      System.out.println(tail);
    } else if ("chmod".equals(cmd)) {
      System.out.println(chmod);
    } else if ("chown".equals(cmd)) {
      System.out.println(chown);
    } else if ("chgrp".equals(cmd)) {
      System.out.println(chgrp);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(fs);
      System.out.println(ls);
      System.out.println(lsr);
      System.out.println(du);
      System.out.println(dus);
      System.out.println(mv);
      System.out.println(cp);
      System.out.println(rm);
      System.out.println(rmr);
      System.out.println(put);
      System.out.println(copyFromLocal);
      System.out.println(moveFromLocal);
      System.out.println(get);
      System.out.println(getmerge);
      System.out.println(cat);
      System.out.println(copyToLocal);
      System.out.println(moveToLocal);
      System.out.println(mkdir);
      System.out.println(setrep);
      System.out.println(chmod);
      System.out.println(chown);      
      System.out.println(chgrp);
      System.out.println(help);
    }        

                           
  }

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int doall(String cmd, String argv[], Configuration conf, 
                    int startindex) {
    int exitCode = 0;
    int i = startindex;
    //
    // for each source file, issue the command
    //
    for (; i < argv.length; i++) {
      try {
        //
        // issue the command to the fs
        //
        if ("-cat".equals(cmd)) {
          cat(argv[i], true);
        } else if ("-mkdir".equals(cmd)) {
          mkdir(argv[i]);
        } else if ("-rm".equals(cmd)) {
          delete(argv[i], false);
        } else if ("-rmr".equals(cmd)) {
          delete(argv[i], true);
        } else if ("-du".equals(cmd)) {
          du(argv[i]);
        } else if ("-dus".equals(cmd)) {
          dus(argv[i]);
        } else if ("-ls".equals(cmd)) {
          ls(argv[i], false);
        } else if ("-lsr".equals(cmd)) {
          ls(argv[i], true);
        } else if ("-touchz".equals(cmd)) {
          touchz(argv[i]);
        } else if ("-text".equals(cmd)) {
          text(argv[i]);
        }
      } catch (RemoteException e) {
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error message.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " +
                             content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        //
        // IO exception encountered locally.
        //
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": " +
                           e.getLocalizedMessage());
      }
    }
    return exitCode;
  }

  /**
   * Displays format of commands.
   * 
   */
  void printUsage(String cmd) {
    if ("-fs".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-fs <local | file system URI>]");
    } else if ("-conf".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-conf <configuration file>]");
    } else if ("-D".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-D <[property=value>]");
    } else if ("-ls".equals(cmd) || "-lsr".equals(cmd) ||
               "-du".equals(cmd) || "-dus".equals(cmd) ||
               "-rm".equals(cmd) || "-rmr".equals(cmd) ||
               "-touchz".equals(cmd) || "-mkdir".equals(cmd) ||
               "-text".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " <path>]");
    } else if ("-mv".equals(cmd) || "-cp".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " <src> <dst>]");
    } else if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd) ||
               "-moveFromLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " <localsrc> <dst>]");
    } else if ("-get".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + GET_SHORT_USAGE + "]"); 
    } else if ("-copyToLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + COPYTOLOCAL_SHORT_USAGE+ "]"); 
    } else if ("-moveToLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " [-crc] <src> <localdst>]");
    } else if ("-cat".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " <src>]");
    } else if ("-setrep".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + SETREP_SHORT_USAGE + "]");
    } else if ("-test".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-test -[ezd] <path>]");
    } else if ("-stat".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-stat [format] <path>]");
    } else if ("-tail".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + TAIL_USAGE + "]");
    } else {
      System.err.println("Usage: java FsShell");
      System.err.println("           [-ls <path>]");
      System.err.println("           [-lsr <path>]");
      System.err.println("           [-du <path>]");
      System.err.println("           [-dus <path>]");
      System.err.println("           [-mv <src> <dst>]");
      System.err.println("           [-cp <src> <dst>]");
      System.err.println("           [-rm <path>]");
      System.err.println("           [-rmr <path>]");
      System.err.println("           [-expunge]");
      System.err.println("           [-put <localsrc> <dst>]");
      System.err.println("           [-copyFromLocal <localsrc> <dst>]");
      System.err.println("           [-moveFromLocal <localsrc> <dst>]");
      System.err.println("           [" + GET_SHORT_USAGE + "]");
      System.err.println("           [-getmerge <src> <localdst> [addnl]]");
      System.err.println("           [-cat <src>]");
      System.err.println("           [-text <src>]");
      System.err.println("           [" + COPYTOLOCAL_SHORT_USAGE + "]");
      System.err.println("           [-moveToLocal [-crc] <src> <localdst>]");
      System.err.println("           [-mkdir <path>]");
      System.err.println("           [" + SETREP_SHORT_USAGE + "]");
      System.err.println("           [-touchz <path>]");
      System.err.println("           [-test -[ezd] <path>]");
      System.err.println("           [-stat [format] <path>]");
      System.err.println("           [" + TAIL_USAGE + "]");
      System.err.println("           [" + FsShellPermissions.CHMOD_USAGE + "]");      
      System.err.println("           [" + FsShellPermissions.CHOWN_USAGE + "]");
      System.err.println("           [" + FsShellPermissions.CHGRP_USAGE + "]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage(""); 
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-put".equals(cmd) || "-test".equals(cmd) ||
        "-copyFromLocal".equals(cmd) || "-moveFromLocal".equals(cmd)) {
      if (argv.length != 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-get".equals(cmd) || 
               "-copyToLocal".equals(cmd) || "-moveToLocal".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-mv".equals(cmd) || "-cp".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-rm".equals(cmd) || "-rmr".equals(cmd) ||
               "-cat".equals(cmd) || "-mkdir".equals(cmd) ||
               "-touchz".equals(cmd) || "-stat".equals(cmd) ||
               "-text".equals(cmd)) {
      if (argv.length < 2) {
        printUsage(cmd);
        return exitCode;
      }
    }

    // initialize FsShell
    try {
      init();
    } catch (RPC.VersionMismatch v) { 
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to FS. command aborted.");
      return exitCode;
    }

    exitCode = 0;
    try {
      if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
        copyFromLocal(new Path(argv[i++]), argv[i++]);
      } else if ("-moveFromLocal".equals(cmd)) {
        moveFromLocal(new Path(argv[i++]), argv[i++]);
      } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
        copyToLocal(argv, i);
      } else if ("-getmerge".equals(cmd)) {
        if (argv.length>i+2)
          copyMergeToLocal(argv[i++], new Path(argv[i++]), Boolean.parseBoolean(argv[i++]));
        else
          copyMergeToLocal(argv[i++], new Path(argv[i++]));
      } else if ("-cat".equals(cmd)) {
        exitCode = doall(cmd, argv, getConf(), i);
      } else if ("-text".equals(cmd)) {
        exitCode = doall(cmd, argv, getConf(), i);
      } else if ("-moveToLocal".equals(cmd)) {
        moveToLocal(argv[i++], new Path(argv[i++]));
      } else if ("-setrep".equals(cmd)) {
        setReplication(argv, i);           
      } else if ("-chmod".equals(cmd) || 
                 "-chown".equals(cmd) ||
                 "-chgrp".equals(cmd)) {
        FsShellPermissions.changePermissions(fs, cmd, argv, i, this);
      } else if ("-ls".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, getConf(), i);
        } else {
          ls(Path.CUR_DIR, false);
        } 
      } else if ("-lsr".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, getConf(), i);
        } else {
          ls(Path.CUR_DIR, true);
        } 
      } else if ("-mv".equals(cmd)) {
        exitCode = rename(argv, getConf());
      } else if ("-cp".equals(cmd)) {
        exitCode = copy(argv, getConf());
      } else if ("-rm".equals(cmd)) {
        exitCode = doall(cmd, argv, getConf(), i);
      } else if ("-rmr".equals(cmd)) {
        exitCode = doall(cmd, argv, getConf(), i);
      } else if ("-expunge".equals(cmd)) {
        expunge();
      } else if ("-du".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, getConf(), i);
        } else {
          du(".");
        }
      } else if ("-dus".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, getConf(), i);
        } else {
          dus(".");
        }         
      } else if ("-mkdir".equals(cmd)) {
        exitCode = doall(cmd, argv, getConf(), i);
      } else if ("-touchz".equals(cmd)) {
        exitCode = doall(cmd, argv, getConf(), i);
      } else if ("-test".equals(cmd)) {
        exitCode = test(argv, i);
      } else if ("-stat".equals(cmd)) {
        if (i + 1 < argv.length) {
          stat(argv[i++].toCharArray(), argv[i++]);
        } else {
          stat("%y".toCharArray(), argv[i]);
        }
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else if ("-tail".equals(cmd)) {
        tail(argv, i);           
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " + 
                           content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " + 
                           ex.getLocalizedMessage());  
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      // 
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + 
                         e.getLocalizedMessage());  
    } catch (RuntimeException re) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage());  
    } finally {
    }
    return exitCode;
  }

  public void close() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    FsShell shell = new FsShell();
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }

  /**
   * Accumulate exceptions if there is any.  Throw them at last.
   */
  private abstract class DelayedExceptionThrowing {
    abstract void process(Path p, FileSystem srcFs) throws IOException;

    final void processSrc(Path srcPattern, FileSystem srcFs
        ) throws IOException {
      List<IOException> exceptions = new ArrayList<IOException>();
      for(Path p : srcFs.globPaths(srcPattern))
        try { process(p, srcFs); } 
        catch(IOException ioe) { exceptions.add(ioe); }
    
      if (!exceptions.isEmpty())
        if (exceptions.size() == 1)
          throw exceptions.get(0);
        else 
          throw new IOException("Multiple IOExceptions: " + exceptions);
    }
  }
}
