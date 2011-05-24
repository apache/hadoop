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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.PathExceptions.PathNotFoundException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Provide command line access to a FileSystem. */
@InterfaceAudience.Private
public class FsShell extends Configured implements Tool {
  
  static final Log LOG = LogFactory.getLog(FsShell.class);

  private FileSystem fs;
  private Trash trash;
  protected CommandFactory commandFactory;

  public static final SimpleDateFormat dateForm = 
    new SimpleDateFormat("yyyy-MM-dd HH:mm");
  static final int BORDER = 2;

  static final String GET_SHORT_USAGE = "-get [-ignoreCrc] [-crc] <src> <localdst>";
  static final String COPYTOLOCAL_SHORT_USAGE = GET_SHORT_USAGE.replace(
      "-get", "-copyToLocal");
  static final String DU_USAGE="-du [-s] [-h] <paths...>";

  /**
   */
  public FsShell() {
    this(null);
  }

  public FsShell(Configuration conf) {
    super(conf);
    fs = null;
    trash = null;
    commandFactory = new CommandFactory();
  }
  
  protected FileSystem getFS() throws IOException {
    if(fs == null)
      fs = FileSystem.get(getConf());
    
    return fs;
  }
  
  protected Trash getTrash() throws IOException {
    if (this.trash == null) {
      this.trash = new Trash(getConf());
    }
    return this.trash;
  }
  
  protected void init() throws IOException {
    getConf().setQuietMode(true);
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
   * Add local files to the indicated FileSystem name. src is kept.
   */
  void copyFromLocal(Path[] srcs, String dstf) throws IOException {
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    if (srcs.length == 1 && srcs[0].toString().equals("-"))
      copyFromStdin(dstPath, dstFs);
    else
      dstFs.copyFromLocalFile(false, false, srcs, dstPath);
  }
  
  /**
   * Add local files to the indicated FileSystem name. src is removed.
   */
  void moveFromLocal(Path[] srcs, String dstf) throws IOException {
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    dstFs.moveFromLocalFile(srcs, dstPath);
  }

  /**
   * Add a local file to the indicated FileSystem name. src is removed.
   */
  void moveFromLocal(Path src, String dstf) throws IOException {
    moveFromLocal((new Path[]{src}), dstf);
  }

  /**
   * Obtain the indicated files that match the file pattern <i>srcf</i>
   * and copy them to the local name. srcf is kept.
   * When copying multiple files, the destination must be a directory. 
   * Otherwise, IOException is thrown.
   * @param argv : arguments
   * @param pos : Ignore everything before argv[pos]  
   * @throws Exception 
   * @see org.apache.hadoop.fs.FileSystem.globStatus 
   */
  void copyToLocal(String[]argv, int pos) throws Exception {
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
    boolean copyCrc = cf.getOpt("crc");
    final boolean verifyChecksum = !cf.getOpt("ignoreCrc");

    if (dststr.equals("-")) {
      if (copyCrc) {
        System.err.println("-crc option is not valid when destination is stdout.");
      }
      
      List<String> catArgv = new ArrayList<String>();
      catArgv.add("-cat");
      if (cf.getOpt("ignoreCrc")) catArgv.add("-ignoreCrc");
      catArgv.add(srcstr);      
      run(catArgv.toArray(new String[0]));
    } else {
      File dst = new File(dststr);      
      Path srcpath = new Path(srcstr);
      FileSystem srcFS = getSrcFileSystem(srcpath, verifyChecksum);
      if (copyCrc && !(srcFS instanceof ChecksumFileSystem)) {
        System.err.println("-crc option is not valid when source file system " +
            "does not have crc files. Automatically turn the option off.");
        copyCrc = false;
      }
      FileStatus[] srcs = srcFS.globStatus(srcpath);
      if (null == srcs) {
        throw new PathNotFoundException(srcstr);
      }
      boolean dstIsDir = dst.isDirectory(); 
      if (srcs.length > 1 && !dstIsDir) {
        throw new IOException("When copying multiple files, "
                              + "destination should be a directory.");
      }
      for (FileStatus status : srcs) {
        Path p = status.getPath();
        File f = dstIsDir? new File(dst, p.getName()): dst;
        copyToLocal(srcFS, status, f, copyCrc);
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
    srcFs.setVerifyChecksum(verifyChecksum);
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
  private void copyToLocal(final FileSystem srcFS, final FileStatus srcStatus,
                           final File dst, final boolean copyCrc)
    throws IOException {
    /* Keep the structure similar to ChecksumFileSystem.copyToLocal(). 
     * Ideal these two should just invoke FileUtil.copy() and not repeat
     * recursion here. Of course, copy() should support two more options :
     * copyCrc and useTmpFile (may be useTmpFile need not be an option).
     */
    
    Path src = srcStatus.getPath();
    if (srcStatus.isFile()) {
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
        if (!(srcFS instanceof ChecksumFileSystem)) {
          throw new IOException("Source file system does not have crc files");
        }
        
        ChecksumFileSystem csfs = (ChecksumFileSystem) srcFS;
        File dstcs = FileSystem.getLocal(srcFS.getConf())
          .pathToFile(csfs.getChecksumFile(new Path(dst.getCanonicalPath())));
        FileSystem fs = csfs.getRawFileSystem();
        FileStatus status = csfs.getFileStatus(csfs.getChecksumFile(src));
        copyToLocal(fs, status, dstcs, false);
      } 
    } else if (srcStatus.isSymlink()) {
      throw new AssertionError("Symlinks unsupported");
    } else {
      // once FileUtil.copy() supports tmp file, we don't need to mkdirs().
      if (!dst.mkdirs()) {
        throw new IOException("Failed to create local destination \"" +
                              dst + "\".");
      }
      for(FileStatus status : srcFS.listStatus(src)) {
        copyToLocal(srcFS, status,
                    new File(dst, status.getPath().getName()), copyCrc);
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
   * Show the size of a partition in the filesystem that contains
   * the specified <i>path</i>.
   * @param path a path specifying the source partition. null means /.
   * @throws IOException  
   */
  void df(String path) throws IOException {
    if (path == null) path = "/";
    final Path srcPath = new Path(path);
    final FileSystem srcFs = srcPath.getFileSystem(getConf());
    if (! srcFs.exists(srcPath)) {
      throw new PathNotFoundException(path);
    }
    final FsStatus stats = srcFs.getStatus(srcPath);
    final int PercentUsed = (int)(100.0f *  (float)stats.getUsed() / (float)stats.getCapacity());
    System.out.println("Filesystem\t\tSize\tUsed\tAvail\tUse%");
    System.out.printf("%s\t\t%d\t%d\t%d\t%d%%\n",
      path, 
      stats.getCapacity(), stats.getUsed(), stats.getRemaining(),
      PercentUsed);
  }

  /**
   * Show the size of all files that match the file pattern <i>src</i>
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void du(String[] cmd, int pos) throws IOException {
    CommandFormat c = new CommandFormat(
      "du", 0, Integer.MAX_VALUE, "h", "s");
    List<String> params;
    try {
      params = c.parse(cmd, pos);
    } catch (IllegalArgumentException iae) {
      System.err.println("Usage: java FsShell " + DU_USAGE);
      throw iae;
    }
    boolean humanReadable = c.getOpt("h");
    boolean summary = c.getOpt("s");

    // Default to cwd
    if (params.isEmpty()) {
      params.add(".");
    }

    List<UsagePair> usages = new ArrayList<UsagePair>();

    for (String src : params) {
      Path srcPath = new Path(src);
      FileSystem srcFs = srcPath.getFileSystem(getConf());
      FileStatus globStatus[] = srcFs.globStatus(srcPath);
      FileStatus statusToPrint[];

      if (summary) {
        statusToPrint = globStatus;
      } else {
        Path statPaths[] = FileUtil.stat2Paths(globStatus, srcPath);
        try {
          statusToPrint = srcFs.listStatus(statPaths);
        } catch(FileNotFoundException fnfe) {
          statusToPrint = null;
        }
      }
      if ((statusToPrint == null) || ((statusToPrint.length == 0) &&
                                      (!srcFs.exists(srcPath)))){
        throw new PathNotFoundException(src);
      }

      if (!summary) {
        System.out.println("Found " + statusToPrint.length + " items");
      }

      for (FileStatus stat : statusToPrint) {
        long length;
        if (summary || stat.isDirectory()) {
          length = srcFs.getContentSummary(stat.getPath()).getLength();
        } else {
          length = stat.getLen();
        }

        usages.add(new UsagePair(String.valueOf(stat.getPath()), length));
      }
    }
    printUsageSummary(usages, humanReadable);
  }
    
  /**
   * Show the summary disk usage of each dir/file 
   * that matches the file pattern <i>src</i>
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void dus(String[] cmd, int pos) throws IOException {
    String newcmd[] = new String[cmd.length + 1];
    System.arraycopy(cmd, 0, newcmd, 0, cmd.length);
    newcmd[cmd.length] = "-s";
    du(newcmd, pos);
  }

  private void printUsageSummary(List<UsagePair> usages,
                                 boolean humanReadable) {
    int maxColumnWidth = 0;
    for (UsagePair usage : usages) {
      String toPrint = humanReadable ?
        StringUtils.humanReadableInt(usage.bytes) : String.valueOf(usage.bytes);
      if (toPrint.length() > maxColumnWidth) {
        maxColumnWidth = toPrint.length();
      }
    }

    for (UsagePair usage : usages) {
      String toPrint = humanReadable ?
        StringUtils.humanReadableInt(usage.bytes) : String.valueOf(usage.bytes);
      System.out.printf("%-"+ (maxColumnWidth + BORDER) +"s", toPrint);
      System.out.println(usage.path);
    }
  }

  /**
   * Check file types.
   */
  int test(String argv[], int i) throws IOException {
    if (!argv[i].startsWith("-") || argv[i].length() > 2)
      throw new IOException("Not a flag: " + argv[i]);
    char flag = argv[i].toCharArray()[1];
    PathData item = new PathData(argv[++i], getConf());
    
    if ((flag != 'e') && !item.exists) { 
      // TODO: it's backwards compat, but why is this throwing an exception?
      // it's not like the shell test cmd
      throw new PathNotFoundException(item.toString());
    }
    switch(flag) {
      case 'e':
        return item.exists ? 0 : 1;
      case 'z':
        return (item.stat.getLen() == 0) ? 0 : 1;
      case 'd':
        return item.stat.isDirectory() ? 0 : 1;
      default:
        throw new IOException("Unknown flag: " + flag);
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
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void rename(String srcf, String dstf) throws IOException {
    Path srcPath = new Path(srcf);
    Path dstPath = new Path(dstf);
    FileSystem fs = srcPath.getFileSystem(getConf());
    URI srcURI = fs.getUri();
    URI dstURI = dstPath.getFileSystem(getConf()).getUri();
    if (srcURI.compareTo(dstURI) != 0) {
      throw new IOException("src and destination filesystems do not match.");
    }
    Path[] srcs = FileUtil.stat2Paths(fs.globStatus(srcPath), srcPath);
    Path dst = new Path(dstf);
    if (srcs.length > 1 && !fs.isDirectory(dst)) {
      throw new IOException("When moving multiple files, " 
                            + "destination should be a directory.");
    }
    for(int i=0; i<srcs.length; i++) {
      if (!fs.rename(srcs[i], dst)) {
        FileStatus srcFstatus = null;
        FileStatus dstFstatus = null;
        try {
          srcFstatus = fs.getFileStatus(srcs[i]);
        } catch(FileNotFoundException e) {
          throw new PathNotFoundException(srcs[i].toString());
        }
        try {
          dstFstatus = fs.getFileStatus(dst);
        } catch(IOException e) {
          LOG.debug("Error getting file status of " + dst, e);
        }
        if((srcFstatus!= null) && (dstFstatus!= null)) {
          if (srcFstatus.isDirectory()  && !dstFstatus.isDirectory()) {
            throw new IOException("cannot overwrite non directory "
                + dst + " with directory " + srcs[i]);
          }
        }
        throw new IOException("Failed to rename " + srcs[i] + " to " + dst);
      }
    }
  }

  /**
   * Move/rename file(s) to a destination file. Multiple source
   * files can be specified. The destination is the last element of
   * the argvp[] array.
   * If multiple source files are specified, then the destination 
   * must be a directory. Otherwise, IOException is thrown.
   * @throws IOException on error
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
        LOG.debug("Error renaming " + argv[i], e);
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error mesage.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " + content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        LOG.debug("Error renaming " + argv[i], e);
        //
        // IO exception encountered locally.
        // 
        exitCode = -1;
        displayError(cmd, e);
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
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void copy(String srcf, String dstf, Configuration conf) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
    Path dstPath = new Path(dstf);
    FileSystem dstFs = dstPath.getFileSystem(getConf());
    Path [] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
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
   * @throws IOException on error
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
      FileSystem pFS = dst.getFileSystem(conf);
      if (!pFS.isDirectory(dst)) {
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
      } catch (IOException e) {
        LOG.debug("Error copying " + argv[i], e);
        exitCode = -1;
        displayError(cmd, e);
      }
    }
    return exitCode;
  }

  /**
   * Delete all files that match the file pattern <i>srcf</i>.
   * @param srcf a file pattern specifying source files
   * @param recursive if need to delete subdirs
   * @param skipTrash Should we skip the trash, if it's enabled?
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  void delete(String srcf, final boolean recursive, final boolean skipTrash) 
                                                            throws IOException {
    //rm behavior in Linux
    //  [~/1207]$ ls ?.txt
    //  x.txt  z.txt
    //  [~/1207]$ rm x.txt y.txt z.txt 
    //  rm: cannot remove `y.txt': No such file or directory

    Path srcPattern = new Path(srcf);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        delete(p, srcFs, recursive, skipTrash);
      }
    }.globAndProcess(srcPattern, srcPattern.getFileSystem(getConf()));
  }
    
  /* delete a file */
  private void delete(Path src, FileSystem srcFs, boolean recursive, 
                      boolean skipTrash) throws IOException {
    FileStatus fs = null;
    try {
      fs = srcFs.getFileStatus(src);
    } catch (FileNotFoundException fnfe) {
      // Have to re-throw so that console output is as expected
      throw new PathNotFoundException(src.toString());
    }
    
    if (fs.isDirectory() && !recursive) {
      throw new IOException("Cannot remove directory \"" + src +
                            "\", use -rmr instead");
    }
    
    if(!skipTrash) {
      try {
        if (Trash.moveToAppropriateTrash(srcFs, src, getConf())) {
          System.out.println("Moved to trash: " + src);
          return;
        }
      } catch (IOException e) {
        LOG.debug("Error with trash", e);
        Exception cause = (Exception) e.getCause();
        String msg = "";
        if(cause != null) {
          msg = cause.getLocalizedMessage();
        }
        System.err.println("Problem with Trash." + msg +". Consider using -skipTrash option");        
        throw e;
      }
    }
    
    if (srcFs.delete(src, true)) {
      System.out.println("Deleted " + src);
    } else {
      throw new IOException("Delete failed " + src);
    }
  }

  private void expunge() throws IOException {
    getTrash().expunge();
    getTrash().checkpoint();
  }

  /**
   * Returns the Trash object associated with this shell.
   */
  public Path getCurrentTrashDir() throws IOException {
    return getTrash().getCurrentTrashDir();
  }

  /**
   * Return an abbreviated English-language desc of the byte length
   * @deprecated Consider using {@link org.apache.hadoop.util.StringUtils#byteDesc} instead.
   */
  @Deprecated
  public static String byteDesc(long len) {
    return StringUtils.byteDesc(len);
  }

  /**
   * @deprecated Consider using {@link org.apache.hadoop.util.StringUtils#limitDecimalTo2} instead.
   */
  @Deprecated
  public static synchronized String limitDecimalTo2(double d) {
    return StringUtils.limitDecimalTo2(d);
  }

  private void printHelp(String cmd) {
    String summary = "hadoop fs is the command to execute fs commands. " +
      "The full syntax is: \n\n" +
      "hadoop fs [-fs <local | file system URI>] [-conf <configuration file>]\n\t" +
      "[-D <property=value>] [-df [<path>]] [-du [-s] [-h] <path>]\n\t" +
      "[-dus <path>] [-mv <src> <dst>] [-cp <src> <dst>] [-rm [-skipTrash] <src>]\n\t" + 
      "[-rmr [-skipTrash] <src>] [-put <localsrc> ... <dst>] [-copyFromLocal <localsrc> ... <dst>]\n\t" +
      "[-moveFromLocal <localsrc> ... <dst>] [" + 
      GET_SHORT_USAGE + "\n\t" +
      "[" + COPYTOLOCAL_SHORT_USAGE + "] [-moveToLocal <src> <localdst>]\n\t" +
      "[-report]\n\t" +
      "[-test -[ezd] <path>]";

    String conf ="-conf <configuration file>:  Specify an application configuration file.";
 
    String D = "-D <property=value>:  Use value for given property.";
  
    String fs = "-fs [local | <file system URI>]: \tSpecify the file system to use.\n" + 
      "\t\tIf not specified, the current configuration is used, \n" +
      "\t\ttaken from the following, in increasing precedence: \n" + 
      "\t\t\tcore-default.xml inside the hadoop jar file \n" +
      "\t\t\tcore-site.xml in $HADOOP_CONF_DIR \n" +
      "\t\t'local' means use the local file system as your DFS. \n" +
      "\t\t<file system URI> specifies a particular file system to \n" +
      "\t\tcontact. This argument is optional but if used must appear\n" +
      "\t\tappear first on the command line.  Exactly one additional\n" +
      "\t\targument must be specified. \n";

    String df = "-df [<path>]: \tShows the capacity, free and used space of the filesystem.\n"+
      "\t\tIf the filesystem has multiple partitions, and no path to a particular partition\n"+
      "\t\tis specified, then the status of the root partitions will be shown.\n";

    String du = "-du [-s] [-h] <path>: \tShow the amount of space, in bytes, used by the files that \n" +
      "\t\tmatch the specified file pattern. The following flags are optional:\n" +
      "\t\t  -s   Rather than showing the size of each individual file that\n" +
      "\t\t       matches the pattern, shows the total (summary) size.\n" +
      "\t\t  -h   Formats the sizes of files in a human-readable fashion\n" +
      "\t\t       rather than a number of bytes.\n" +
      "\n" + 
      "\t\tNote that, even without the -s option, this only shows size summaries\n" +
      "\t\tone level deep into a directory.\n" +
      "\t\tThe output is in the form \n" + 
      "\t\t\tsize\tname(full path)\n"; 

    String dus = "-dus <path>: \tShow the amount of space, in bytes, used by the files that \n" +
      "\t\tmatch the specified file pattern. This is equivalent to -du -s above.\n";
    
    String mv = "-mv <src> <dst>:   Move files that match the specified file pattern <src>\n" +
      "\t\tto a destination <dst>.  When moving multiple files, the \n" +
      "\t\tdestination must be a directory. \n";

    String cp = "-cp <src> <dst>:   Copy files that match the file pattern <src> to a \n" +
      "\t\tdestination.  When copying multiple files, the destination\n" +
      "\t\tmust be a directory. \n";

    String rm = "-rm [-skipTrash] <src>: \tDelete all files that match the specified file pattern.\n" +
      "\t\tEquivalent to the Unix command \"rm <src>\"\n" +
      "\t\t-skipTrash option bypasses trash, if enabled, and immediately\n" +
      "deletes <src>";

    String rmr = "-rmr [-skipTrash] <src>: \tRemove all directories which match the specified file \n" +
      "\t\tpattern. Equivalent to the Unix command \"rm -rf <src>\"\n" +
      "\t\t-skipTrash option bypasses trash, if enabled, and immediately\n" +
      "deletes <src>";

    String put = "-put <localsrc> ... <dst>: \tCopy files " + 
    "from the local file system \n\t\tinto fs. \n";

    String copyFromLocal = "-copyFromLocal <localsrc> ... <dst>:" +
    " Identical to the -put command.\n";

    String moveFromLocal = "-moveFromLocal <localsrc> ... <dst>:" +
    " Same as -put, except that the source is\n\t\tdeleted after it's copied.\n"; 

    String get = GET_SHORT_USAGE
      + ":  Copy files that match the file pattern <src> \n" +
      "\t\tto the local name.  <src> is kept.  When copying mutiple, \n" +
      "\t\tfiles, the destination must be a directory. \n";

    String copyToLocal = COPYTOLOCAL_SHORT_USAGE
                         + ":  Identical to the -get command.\n";

    String moveToLocal = "-moveToLocal <src> <localdst>:  Not implemented yet \n";
        
    String test = "-test -[ezd] <path>: If file { exists, has zero length, is a directory\n" +
      "\t\tthen return 0, else return 1.\n";

    String expunge = "-expunge: Empty the Trash.\n";
    
    String help = "-help [cmd]: \tDisplays help for given command or all commands if none\n" +
      "\t\tis specified.\n";

    Command instance = commandFactory.getInstance("-" + cmd);
    if (instance != null) {
      printHelp(instance);
    } else if ("fs".equals(cmd)) {
      System.out.println(fs);
    } else if ("conf".equals(cmd)) {
      System.out.println(conf);
    } else if ("D".equals(cmd)) {
      System.out.println(D);
    } else if ("df".equals(cmd)) {
      System.out.println(df);
    } else if ("du".equals(cmd)) {
      System.out.println(du);
    } else if ("dus".equals(cmd)) {
      System.out.println(dus);
    } else if ("rm".equals(cmd)) {
      System.out.println(rm);
    } else if ("expunge".equals(cmd)) {
      System.out.println(expunge);
    } else if ("rmr".equals(cmd)) {
      System.out.println(rmr);
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
    } else if ("copyToLocal".equals(cmd)) {
      System.out.println(copyToLocal);
    } else if ("moveToLocal".equals(cmd)) {
      System.out.println(moveToLocal);
    } else if ("get".equals(cmd)) {
      System.out.println(get);
    } else if ("test".equals(cmd)) {
      System.out.println(test);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      for (String thisCmdName : commandFactory.getNames()) {
        instance = commandFactory.getInstance(thisCmdName);
        System.out.println("\t[" + instance.getUsage() + "]");
      }
      System.out.println("\t[-help [cmd]]\n");
      
      System.out.println(fs);
      System.out.println(df);
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
      System.out.println(copyToLocal);
      System.out.println(moveToLocal);
      System.out.println(test);

      for (String thisCmdName : commandFactory.getNames()) {
        printHelp(commandFactory.getInstance(thisCmdName));
      }

      System.out.println(help);
    }        
  }

  // TODO: will eventually auto-wrap the text, but this matches the expected
  // output for the hdfs tests...
  private void printHelp(Command instance) {
    boolean firstLine = true;
    for (String line : instance.getDescription().split("\n")) {
      String prefix;
      if (firstLine) {
        prefix = instance.getUsage() + ":\t";
        firstLine = false;
      } else {
        prefix = "\t\t";
      }
      System.out.println(prefix + line);
    }    
  }
  
  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int doall(String cmd, String argv[], int startindex) {
    int exitCode = 0;
    int i = startindex;
    boolean rmSkipTrash = false;
    
    // Check for -skipTrash option in rm/rmr
    if(("-rm".equals(cmd) || "-rmr".equals(cmd)) 
        && "-skipTrash".equals(argv[i])) {
      rmSkipTrash = true;
      i++;
    }
    
    //
    // for each source file, issue the command
    //
    for (; i < argv.length; i++) {
      try {
        //
        // issue the command to the fs
        //
        if ("-rm".equals(cmd)) {
          delete(argv[i], false, rmSkipTrash);
        } else if ("-rmr".equals(cmd)) {
          delete(argv[i], true, rmSkipTrash);
        } else if ("-df".equals(cmd)) {
          df(argv[i]);
        }
      } catch (IOException e) {
        LOG.debug("Error", e);
        exitCode = -1;
        displayError(cmd, e);
      }
    }
    return exitCode;
  }

  /**
   * Displays format of commands.
   * 
   */
  private void printUsage(String cmd) {
    String prefix = "Usage: java " + FsShell.class.getSimpleName();

    Command instance = commandFactory.getInstance(cmd);
    if (instance != null) {
      System.err.println(prefix + " [" + instance.getUsage() + "]");
    } else if ("-fs".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-fs <local | file system URI>]");
    } else if ("-conf".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-conf <configuration file>]");
    } else if ("-D".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [-D <[property=value>]");
    } else if ("-du".equals(cmd) || "-dus".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " <path>]");
    } else if ("-df".equals(cmd) ) {
      System.err.println("Usage: java FsShell" +
                         " [" + cmd + " [<path>]]");
    } else if ("-rm".equals(cmd) || "-rmr".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + cmd + 
                           " [-skipTrash] <src>]");
    } else if ("-mv".equals(cmd) || "-cp".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " <src> <dst>]");
    } else if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd) ||
               "-moveFromLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " <localsrc> ... <dst>]");
    } else if ("-get".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + GET_SHORT_USAGE + "]"); 
    } else if ("-copyToLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell [" + COPYTOLOCAL_SHORT_USAGE+ "]"); 
    } else if ("-moveToLocal".equals(cmd)) {
      System.err.println("Usage: java FsShell" + 
                         " [" + cmd + " [-crc] <src> <localdst>]");
    } else if ("-test".equals(cmd)) {
      System.err.println("Usage: java FsShell" +
                         " [-test -[ezd] <path>]");
    } else {
      System.err.println("Usage: java FsShell");
      System.err.println("           [-df [<path>]]");
      System.err.println("           [-du [-s] [-h] <path>]");
      System.err.println("           [-dus <path>]");
      System.err.println("           [-mv <src> <dst>]");
      System.err.println("           [-cp <src> <dst>]");
      System.err.println("           [-rm [-skipTrash] <path>]");
      System.err.println("           [-rmr [-skipTrash] <path>]");
      System.err.println("           [-expunge]");
      System.err.println("           [-put <localsrc> ... <dst>]");
      System.err.println("           [-copyFromLocal <localsrc> ... <dst>]");
      System.err.println("           [-moveFromLocal <localsrc> ... <dst>]");
      System.err.println("           [" + GET_SHORT_USAGE + "]");
      System.err.println("           [" + COPYTOLOCAL_SHORT_USAGE + "]");
      System.err.println("           [-moveToLocal [-crc] <src> <localdst>]");
      System.err.println("           [-test -[ezd] <path>]");
      for (String name : commandFactory.getNames()) {
      	instance = commandFactory.getInstance(name);
        System.err.println("           [" + instance.getUsage() + "]");
      }
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {
    // TODO: This isn't the best place, but this class is being abused with
    // subclasses which of course override this method.  There really needs
    // to be a better base class for all commands
    commandFactory.setConf(getConf());
    commandFactory.registerCommands(FsCommand.class);
    
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
      if (argv.length < 3) {
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
    } else if ("-rm".equals(cmd) || "-rmr".equals(cmd)) {
      if (argv.length < 2) {
        printUsage(cmd);
        return exitCode;
      }
    }
    // initialize FsShell
    try {
      init();
    } catch (RPC.VersionMismatch v) {
      LOG.debug("Version mismatch", v);
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      LOG.debug("Error", e);
      System.err.println("Bad connection to FS. Command aborted. Exception: " +
          e.getLocalizedMessage());
      return exitCode;
    }

    exitCode = 0;
    try {
      Command instance = commandFactory.getInstance(cmd);
      if (instance != null) {
        try {
          exitCode = instance.run(Arrays.copyOfRange(argv, i, argv.length));
        } catch (Exception e) {
          exitCode = -1;
          LOG.debug("Error", e);
          instance.displayError(e);
          if (e instanceof IllegalArgumentException) {
            printUsage(cmd);
          }
        }
      } else if ("-put".equals(cmd) || "-copyFromLocal".equals(cmd)) {
        Path[] srcs = new Path[argv.length-2];
        for (int j=0 ; i < argv.length-1 ;) 
          srcs[j++] = new Path(argv[i++]);
        copyFromLocal(srcs, argv[i++]);
      } else if ("-moveFromLocal".equals(cmd)) {
        Path[] srcs = new Path[argv.length-2];
        for (int j=0 ; i < argv.length-1 ;) 
          srcs[j++] = new Path(argv[i++]);
        moveFromLocal(srcs, argv[i++]);
      } else if ("-get".equals(cmd) || "-copyToLocal".equals(cmd)) {
        copyToLocal(argv, i);
      } else if ("-moveToLocal".equals(cmd)) {
        moveToLocal(argv[i++], new Path(argv[i++]));
      } else if ("-mv".equals(cmd)) {
        exitCode = rename(argv, getConf());
      } else if ("-cp".equals(cmd)) {
        exitCode = copy(argv, getConf());
      } else if ("-rm".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-rmr".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-expunge".equals(cmd)) {
        expunge();
      } else if ("-df".equals(cmd)) {
        if (argv.length-1 > 0) {
          exitCode = doall(cmd, argv, i);
        } else {
          df(null);
        }
      } else if ("-du".equals(cmd)) {
        du(argv, i);
      } else if ("-dus".equals(cmd)) {
        dus(argv, i);
      } else if ("-test".equals(cmd)) {
        exitCode = test(argv, i);
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      LOG.debug("Error", arge);
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (Exception re) {
      LOG.debug("Error", re);
      exitCode = -1;
      displayError(cmd, re);
    } finally {
    }
    return exitCode;
  }

  // TODO: this is a quick workaround to accelerate the integration of
  // redesigned commands.  this will be removed this once all commands are
  // converted.  this change will avoid having to change the hdfs tests
  // every time a command is converted to use path-based exceptions
  private static Pattern[] fnfPatterns = {
    Pattern.compile("File (.*) does not exist\\."),
    Pattern.compile("File does not exist: (.*)"),
    Pattern.compile("`(.*)': specified destination directory doest not exist")
  };
  private void displayError(String cmd, Exception e) {
    String message = e.getLocalizedMessage().split("\n")[0];
    for (Pattern pattern : fnfPatterns) {
      Matcher matcher = pattern.matcher(message);
      if (matcher.matches()) {
        message = new PathNotFoundException(matcher.group(1)).getMessage();
        break;
      }
    }
    System.err.println(cmd.substring(1) + ": " + message);  
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

    final void globAndProcess(Path srcPattern, FileSystem srcFs
        ) throws IOException {
      List<IOException> exceptions = new ArrayList<IOException>();
      for(Path p : FileUtil.stat2Paths(srcFs.globStatus(srcPattern), 
                                       srcPattern))
        try { process(p, srcFs); } 
        catch(IOException ioe) { exceptions.add(ioe); }
    
      if (!exceptions.isEmpty())
        if (exceptions.size() == 1)
          throw exceptions.get(0);
        else 
          throw new IOException("Multiple IOExceptions: " + exceptions);
    }
  }


  /**
   * Utility class for a line of du output
   */
  private static class UsagePair {
    public String path;
    public long bytes;

    public UsagePair(String path, long bytes) {
      this.path = path;
      this.bytes = bytes;
    }
  }
}
