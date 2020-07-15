/*
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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

/**
 * For validating {@link FSImage}.
 * This tool will load the user specified {@link FSImage},
 * build the namespace tree,
 * and then run validations over the namespace tree.
 *
 * The main difference of this tool and
 * {@link org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer}
 * is that
 * {@link org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer}
 * only loads {@link FSImage} but it does not build the namespace tree.
 * Therefore, running validations over the namespace tree is impossible in
 * {@link org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer}.
 */
public class FsImageValidation {
  static final String FS_IMAGE = "FS_IMAGE";

  static HdfsConfiguration newHdfsConfiguration() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    final int aDay = 24*3600_000;
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY, aDay);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY, aDay);
    return conf;
  }

  static FsImageValidation newInstance(String... args) {
    final String f = Cli.parse(args);
    if (f == null) {
      throw new HadoopIllegalArgumentException(
          FS_IMAGE + " is not specified.");
    }
    return new FsImageValidation(new File(f));
  }

  private final File fsImage;

  FsImageValidation(File fsImage) {
    this.fsImage = fsImage;
  }

  int checkINodeReference() throws Exception {
    Cli.println("Check INodeReference for %s", fsImage);

    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        final double percent = NameNode.getStartupProgress().createView()
            .getPercentComplete(Phase.LOADING_FSIMAGE);
        Cli.println("%s Progress: %.1f%%", Phase.LOADING_FSIMAGE, 100*percent);
      }
    };

    final HdfsConfiguration conf = newHdfsConfiguration();

    INodeReferenceValidation.start();
    final Timer t = new Timer();
    t.scheduleAtFixedRate(checkProgress, 0, 60_000);
    if (fsImage.isDirectory()) {
      Cli.println("Loading %s as a directory.", fsImage);
      final String dir = fsImage.getCanonicalPath();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dir);
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, dir);
      FSNamesystem.loadFromDisk(conf);
    } else {
      Cli.println("Loading %s as a file.", fsImage);
      final FSImage fsImage = new FSImage(conf);
      final FSNamesystem namesystem = new FSNamesystem(conf, fsImage, false);

      final NamespaceInfo namespaceInfo = NNStorage.newNamespaceInfo();
      namespaceInfo.clusterID = "cluster0";
      fsImage.getStorage().setStorageInfo(namespaceInfo);

      final FSImageFormat.LoaderDelegator loader
          = FSImageFormat.newLoader(conf, namesystem);
      namesystem.writeLock();
      namesystem.getFSDirectory().writeLock();
      try {
        loader.load(this.fsImage, false);
      } finally {
        namesystem.getFSDirectory().writeUnlock();
        namesystem.writeUnlock();
      }
    }
    t.cancel();
    return INodeReferenceValidation.end();
  }

  static class Cli extends Configured implements Tool {
    static final String COMMAND;
    static final String USAGE;
    static {
      final String clazz = FsImageValidation.class.getSimpleName();
      COMMAND = Character.toLowerCase(clazz.charAt(0)) + clazz.substring(1);
      USAGE = "Usage: hdfs " + COMMAND + " <" + FS_IMAGE + ">";
    }

    @Override
    public int run(String[] args) throws Exception {
      final FsImageValidation validation = FsImageValidation.newInstance(args);
      final int errorCount = validation.checkINodeReference();
      println("Error Count: %s", errorCount);
      return errorCount == 0? 0: 1;
    }

    static String parse(String... args) {
      final String f;
      if (args == null || args.length == 0) {
        f = System.getenv().get(FS_IMAGE);
        if (f != null) {
          println("Environment variable %s = %s", FS_IMAGE, f);
        }
      } else if (args.length == 1) {
        f = args[0];
      } else {
        throw new HadoopIllegalArgumentException(
            "args = " + Arrays.toString(args));
      }

      println("%s = %s", FS_IMAGE, f);
      return f;
    }

    static void println(String format, Object... args) {
      final String s = String.format(format, args);
      System.out.println(s);
    }

    static void printError(String message, Throwable t) {
      System.out.println(message);
      if (t != null) {
        t.printStackTrace(System.out);
      }
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, Cli.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (HadoopIllegalArgumentException e) {
      e.printStackTrace(System.err);
      System.err.println(Cli.USAGE);
      System.exit(-1);
      ToolRunner.printGenericCommandUsage(System.err);
    } catch (Throwable e) {
      Cli.printError("Failed to run " + Cli.COMMAND, e);
      System.exit(-2);
    }
  }
}
