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

package org.apache.hadoop.fs.shell;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.io.IOUtils;

/** Various commands for copy files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class CopyCommands {  
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Merge.class, "-getmerge");
    factory.addClass(Cp.class, "-cp");
    factory.addClass(CopyFromLocal.class, "-copyFromLocal");
    factory.addClass(CopyToLocal.class, "-copyToLocal");
    factory.addClass(Get.class, "-get");
    factory.addClass(Put.class, "-put");
    factory.addClass(AppendToFile.class, "-appendToFile");
  }

  /** merge multiple files together */
  public static class Merge extends FsCommand {
    public static final String NAME = "getmerge";    
    public static final String USAGE = "[-nl] [-skip-empty-file] "
        + "<src> <localdst>";
    public static final String DESCRIPTION =
        "Get all the files in the directories that "
        + "match the source file pattern and merge and sort them to only "
        + "one file on local fs. <src> is kept.\n"
        + "-nl: Add a newline character at the end of each file.\n"
        + "-skip-empty-file: Do not add new line character for empty file.";

    protected PathData dst = null;
    protected String delimiter = null;
    private boolean skipEmptyFileDelimiter;
    protected List<PathData> srcs = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      try {
        CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "nl",
            "skip-empty-file");
        cf.parse(args);

        delimiter = cf.getOpt("nl") ? "\n" : null;
        skipEmptyFileDelimiter = cf.getOpt("skip-empty-file");

        dst = new PathData(new URI(args.removeLast()), getConf());
        if (dst.exists && dst.stat.isDirectory()) {
          throw new PathIsDirectoryException(dst.toString());
        }
        srcs = new LinkedList<PathData>();
      } catch (URISyntaxException e) {
        throw new IOException("unexpected URISyntaxException", e);
      }
    }

    @Override
    protected void processArguments(LinkedList<PathData> items)
    throws IOException {
      super.processArguments(items);
      if (exitCode != 0) { // check for error collecting paths
        return;
      }
      FSDataOutputStream out = dst.fs.create(dst.path);
      try {
        for (PathData src : srcs) {
          if (src.stat.getLen() != 0) {
            try (FSDataInputStream in = src.fs.open(src.path)) {
              IOUtils.copyBytes(in, out, getConf(), false);
              writeDelimiter(out);
            }
          } else if (!skipEmptyFileDelimiter) {
            writeDelimiter(out);
          }
        }
      } finally {
        out.close();
      }
    }

    private void writeDelimiter(FSDataOutputStream out) throws IOException {
      if (delimiter != null) {
        out.write(delimiter.getBytes("UTF-8"));
      }
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
      exitCode = 1; // flag that a path is bad
      super.processNonexistentPath(item);
    }

    // this command is handled a bit differently than others.  the paths
    // are batched up instead of actually being processed.  this avoids
    // unnecessarily streaming into the merge file and then encountering
    // a path error that should abort the merge
    
    @Override
    protected void processPath(PathData src) throws IOException {
      // for directories, recurse one level to get its files, else skip it
      if (src.stat.isDirectory()) {
        if (getDepth() == 0) {
          recursePath(src);
        } // skip subdirs
      } else {
        srcs.add(src);
      }
    }

    @Override
    protected boolean isSorted() {
      //Sort the children for merge
      return true;
    }
  }

  static class Cp extends CommandWithDestination {
    public static final String NAME = "cp";
    public static final String USAGE =
        "[-f] [-p | -p[topax]] [-d] <src> ... <dst>";
    public static final String DESCRIPTION =
      "Copy files that match the file pattern <src> to a " +
      "destination.  When copying multiple files, the destination " +
      "must be a directory. Passing -p preserves status " +
      "[topax] (timestamps, ownership, permission, ACLs, XAttr). " +
      "If -p is specified with no <arg>, then preserves " +
      "timestamps, ownership, permission. If -pa is specified, " +
      "then preserves permission also because ACL is a super-set of " +
      "permission. Passing -f overwrites the destination if it " +
      "already exists. raw namespace extended attributes are preserved " +
      "if (1) they are supported (HDFS only) and, (2) all of the source and " +
      "target pathnames are in the /.reserved/raw hierarchy. raw namespace " +
      "xattr preservation is determined solely by the presence (or absence) " +
        "of the /.reserved/raw prefix and not by the -p option. Passing -d "+
        "will skip creation of temporary file(<dst>._COPYING_).\n";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      popPreserveOption(args);
      CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "f", "d");
      cf.parse(args);
      setDirectWrite(cf.getOpt("d"));
      setOverwrite(cf.getOpt("f"));
      // should have a -r option
      setRecursive(true);
      getRemoteDestination(args);
    }
    
    private void popPreserveOption(List<String> args) {
      for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
        String cur = iter.next();
        if (cur.equals("--")) {
          // stop parsing arguments when you see --
          break;
        } else if (cur.startsWith("-p")) {
          iter.remove();
          if (cur.length() == 2) {
            setPreserve(true);
          } else {
            String attributes = cur.substring(2);
            for (int index = 0; index < attributes.length(); index++) {
              preserve(FileAttribute.getAttribute(attributes.charAt(index)));
            }
          }
          return;
        }
      }
    }
  }
  
  /** 
   * Copy local files to a remote filesystem
   */
  public static class Get extends CommandWithDestination {
    public static final String NAME = "get";
    public static final String USAGE =
      "[-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>";
    public static final String DESCRIPTION =
      "Copy files that match the file pattern <src> " +
      "to the local name.  <src> is kept.  When copying multiple " +
      "files, the destination must be a directory. Passing " +
      "-f overwrites the destination if it already exists and " +
      "-p preserves access and modification times, " +
      "ownership and the mode.\n";

    @Override
    protected void processOptions(LinkedList<String> args)
    throws IOException {
      CommandFormat cf = new CommandFormat(
          1, Integer.MAX_VALUE, "crc", "ignoreCrc", "p", "f");
      cf.parse(args);
      setWriteChecksum(cf.getOpt("crc"));
      setVerifyChecksum(!cf.getOpt("ignoreCrc"));
      setPreserve(cf.getOpt("p"));
      setOverwrite(cf.getOpt("f"));
      setRecursive(true);
      getLocalDestination(args);
    }
  }

  /**
   *  Copy local files to a remote filesystem
   */
  public static class Put extends CommandWithDestination {
    public static final String NAME = "put";
    public static final String USAGE =
        "[-f] [-p] [-l] [-d] <localsrc> ... <dst>";
    public static final String DESCRIPTION =
      "Copy files from the local file system " +
      "into fs. Copying fails if the file already " +
      "exists, unless the -f flag is given.\n" +
      "Flags:\n" +
      "  -p : Preserves access and modification times, ownership and the mode.\n" +
      "  -f : Overwrites the destination if it already exists.\n" +
      "  -l : Allow DataNode to lazily persist the file to disk. Forces\n" +
      "       replication factor of 1. This flag will result in reduced\n" +
      "       durability. Use with care.\n" +
        "  -d : Skip creation of temporary file(<dst>._COPYING_).\n";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf =
          new CommandFormat(1, Integer.MAX_VALUE, "f", "p", "l", "d");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      setLazyPersist(cf.getOpt("l"));
      setDirectWrite(cf.getOpt("d"));
      getRemoteDestination(args);
      // should have a -r option
      setRecursive(true);
    }

    // commands operating on local paths have no need for glob expansion
    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
      List<PathData> items = new LinkedList<PathData>();
      try {
        items.add(new PathData(new URI(arg), getConf()));
      } catch (URISyntaxException e) {
        items.add(new PathData(arg, getConf()));
      }
      return items;
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
      // NOTE: this logic should be better, mimics previous implementation
      if (args.size() == 1 && args.get(0).toString().equals("-")) {
        copyStreamToTarget(System.in, getTargetPath(args.get(0)));
        return;
      }
      super.processArguments(args);
    }
  }

  public static class CopyFromLocal extends Put {
    private ThreadPoolExecutor executor = null;
    private int numThreads = 1;

    private static final int MAX_THREADS =
        Runtime.getRuntime().availableProcessors() * 2;
    public static final String NAME = "copyFromLocal";
    public static final String USAGE =
        "[-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>";
    public static final String DESCRIPTION =
        "Copy files from the local file system " +
        "into fs. Copying fails if the file already " +
        "exists, unless the -f flag is given.\n" +
        "Flags:\n" +
        "  -p : Preserves access and modification times, ownership and the" +
        " mode.\n" +
        "  -f : Overwrites the destination if it already exists.\n" +
        "  -t <thread count> : Number of threads to be used, default is 1.\n" +
        "  -l : Allow DataNode to lazily persist the file to disk. Forces" +
        " replication factor of 1. This flag will result in reduced" +
        " durability. Use with care.\n" +
        "  -d : Skip creation of temporary file(<dst>._COPYING_).\n";

    private void setNumberThreads(String numberThreadsString) {
      if (numberThreadsString == null) {
        numThreads = 1;
      } else {
        int parsedValue = Integer.parseInt(numberThreadsString);
        if (parsedValue <= 1) {
          numThreads = 1;
        } else if (parsedValue > MAX_THREADS) {
          numThreads = MAX_THREADS;
        } else {
          numThreads = parsedValue;
        }
      }
    }

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf =
          new CommandFormat(1, Integer.MAX_VALUE, "f", "p", "l", "d");
      cf.addOptionWithValue("t");
      cf.parse(args);
      setNumberThreads(cf.getOptValue("t"));
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      setLazyPersist(cf.getOpt("l"));
      setDirectWrite(cf.getOpt("d"));
      getRemoteDestination(args);
      // should have a -r option
      setRecursive(true);
    }

    private void copyFile(PathData src, PathData target) throws IOException {
      if (isPathRecursable(src)) {
        throw new PathIsDirectoryException(src.toString());
      }
      super.copyFileToTarget(src, target);
    }

    @Override
    protected void copyFileToTarget(PathData src, PathData target)
        throws IOException {
      // if number of thread is 1, mimic put and avoid threading overhead
      if (numThreads == 1) {
        copyFile(src, target);
        return;
      }

      Runnable task = () -> {
        try {
          copyFile(src, target);
        } catch (IOException e) {
          displayError(e);
        }
      };
      executor.submit(task);
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
        throws IOException {
      executor = new ThreadPoolExecutor(numThreads, numThreads, 1,
          TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024),
          new ThreadPoolExecutor.CallerRunsPolicy());
      super.processArguments(args);

      // issue the command and then wait for it to finish
      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        executor.shutdownNow();
        displayError(e);
        Thread.currentThread().interrupt();
      }
    }

    @VisibleForTesting
    public int getNumThreads() {
      return numThreads;
    }

    @VisibleForTesting
    public ThreadPoolExecutor getExecutor() {
      return executor;
    }
  }
 
  public static class CopyToLocal extends Get {
    public static final String NAME = "copyToLocal";
    public static final String USAGE = Get.USAGE;
    public static final String DESCRIPTION = "Identical to the -get command.";
  }

  /**
   *  Append the contents of one or more local files to a remote
   *  file.
   */
  public static class AppendToFile extends CommandWithDestination {
    public static final String NAME = "appendToFile";
    public static final String USAGE = "<localsrc> ... <dst>";
    public static final String DESCRIPTION =
        "Appends the contents of all the given local files to the " +
            "given dst file. The dst file will be created if it does " +
            "not exist. If <localSrc> is -, then the input is read " +
            "from stdin.";

    private static final int DEFAULT_IO_LENGTH = 1024 * 1024;
    boolean readStdin = false;

    // commands operating on local paths have no need for glob expansion
    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
      List<PathData> items = new LinkedList<PathData>();
      if (arg.equals("-")) {
        readStdin = true;
      } else {
        try {
          items.add(new PathData(new URI(arg), getConf()));
        } catch (URISyntaxException e) {
          if (Path.WINDOWS) {
            // Unlike URI, PathData knows how to parse Windows drive-letter paths.
            items.add(new PathData(arg, getConf()));
          } else {
            throw new IOException("Unexpected URISyntaxException: " + e.toString());
          }
        }
      }
      return items;
    }

    @Override
    protected void processOptions(LinkedList<String> args)
        throws IOException {

      if (args.size() < 2) {
        throw new IOException("missing destination argument");
      }

      getRemoteDestination(args);
      super.processOptions(args);
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
        throws IOException {

      if (!dst.exists) {
        dst.fs.create(dst.path, false).close();
      }

      FileInputStream is = null;
      try (FSDataOutputStream fos = dst.fs.append(dst.path)) {
        if (readStdin) {
          if (args.size() == 0) {
            IOUtils.copyBytes(System.in, fos, DEFAULT_IO_LENGTH);
          } else {
            throw new IOException(
                "stdin (-) must be the sole input argument when present");
          }
        }

        // Read in each input file and write to the target.
        for (PathData source : args) {
          is = new FileInputStream(source.toFile());
          IOUtils.copyBytes(is, fos, DEFAULT_IO_LENGTH);
          IOUtils.closeStream(is);
          is = null;
        }
      } finally {
        if (is != null) {
          IOUtils.closeStream(is);
        }
      }
    }
  }
}
