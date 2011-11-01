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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileUtil;

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
  }

  /** merge multiple files together */
  public static class Merge extends FsCommand {
    public static final String NAME = "getmerge";    
    public static final String USAGE = "<src> <localdst> [addnl]";
    public static final String DESCRIPTION =
      "Get all the files in the directories that\n" +
      "match the source file pattern and merge and sort them to only\n" +
      "one file on local fs. <src> is kept.";

    protected PathData dst = null;
    protected String delimiter = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(2, 3);
      cf.parse(args);

      // TODO: this really should be a -nl option
      if ((args.size() > 2) && Boolean.parseBoolean(args.removeLast())) {
        delimiter = "\n";
      } else {
        delimiter = null;
      }

      dst = new PathData(new File(args.removeLast()), getConf());
    }

    @Override
    protected void processPath(PathData src) throws IOException {
      FileUtil.copyMerge(src.fs, src.path,
          dst.fs, dst.path, false, getConf(), delimiter);
    }
  }

  static class Cp extends CommandWithDestination {
    public static final String NAME = "cp";
    public static final String USAGE = "<src> ... <dst>";
    public static final String DESCRIPTION =
      "Copy files that match the file pattern <src> to a\n" +
      "destination.  When copying multiple files, the destination\n" +
      "must be a directory.";
    
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "f");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      // should have a -r option
      setRecursive(true);
      getRemoteDestination(args);
    }
  }
  
  /** 
   * Copy local files to a remote filesystem
   */
  public static class Get extends CommandWithDestination {
    public static final String NAME = "get";
    public static final String USAGE =
      "[-ignoreCrc] [-crc] <src> ... <localdst>";
    public static final String DESCRIPTION =
      "Copy files that match the file pattern <src>\n" +
      "to the local name.  <src> is kept.  When copying multiple,\n" +
      "files, the destination must be a directory.";

    /**
     * The prefix for the tmp file used in copyToLocal.
     * It must be at least three characters long, required by
     * {@link java.io.File#createTempFile(String, String, File)}.
     */
    private boolean copyCrc;
    private boolean verifyChecksum;

    @Override
    protected void processOptions(LinkedList<String> args)
    throws IOException {
      CommandFormat cf = new CommandFormat(
          1, Integer.MAX_VALUE, "crc", "ignoreCrc");
      cf.parse(args);
      copyCrc = cf.getOpt("crc");
      verifyChecksum = !cf.getOpt("ignoreCrc");
      
      setRecursive(true);
      getLocalDestination(args);
    }

    @Override
    protected void copyFileToTarget(PathData src, PathData target)
    throws IOException {
      src.fs.setVerifyChecksum(verifyChecksum);

      if (copyCrc && !(src.fs instanceof ChecksumFileSystem)) {
        displayWarning(src.fs + ": Does not support checksums");
        copyCrc = false;
      }      

      super.copyFileToTarget(src, target);
      if (copyCrc) {
        // should we delete real file if crc copy fails?
        super.copyFileToTarget(src.getChecksumFile(), target.getChecksumFile());
      }
    }
  }

  /**
   *  Copy local files to a remote filesystem
   */
  public static class Put extends CommandWithDestination {
    public static final String NAME = "put";
    public static final String USAGE = "<localsrc> ... <dst>";
    public static final String DESCRIPTION =
      "Copy files from the local file system\n" +
      "into fs.";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, "f");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      getRemoteDestination(args);
      // should have a -r option
      setRecursive(true);
    }

    // commands operating on local paths have no need for glob expansion
    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
      List<PathData> items = new LinkedList<PathData>();
      items.add(new PathData(new File(arg), getConf()));
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
    public static final String NAME = "copyFromLocal";
    public static final String USAGE = Put.USAGE;
    public static final String DESCRIPTION = "Identical to the -put command.";
  }
 
  public static class CopyToLocal extends Get {
    public static final String NAME = "copyToLocal";
    public static final String USAGE = Get.USAGE;
    public static final String DESCRIPTION = "Identical to the -get command.";
  }
}