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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.util.Time.now;

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
  static final Logger LOG = LoggerFactory.getLogger(FsImageValidation.class);

  static final String FS_IMAGE = "FS_IMAGE";

  static FsImageValidation newInstance(String... args) {
    final String f = Cli.parse(args);
    if (f == null) {
      throw new HadoopIllegalArgumentException(
          FS_IMAGE + " is not specified.");
    }
    return new FsImageValidation(new File(f));
  }

  static void initConf(Configuration conf) {
    final int aDay = 24*3600_000;
    conf.setInt(DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY, aDay);
    conf.setInt(DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY, aDay);
    conf.setBoolean(DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, false);
  }

  /** Set (fake) HA so that edit logs will not be loaded. */
  static void setHaConf(String nsId, Configuration conf) {
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsId);
    final String haNNKey = DFS_HA_NAMENODES_KEY_PREFIX + "." + nsId;
    conf.set(haNNKey, "nn0,nn1");
    final String rpcKey = DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nsId + ".";
    conf.set(rpcKey + "nn0", "127.0.0.1:8080");
    conf.set(rpcKey + "nn1", "127.0.0.1:8080");
  }

  static void initLogLevels() {
    Util.setLogLevel(FSImage.class, Level.TRACE);
    Util.setLogLevel(FileJournalManager.class, Level.TRACE);

    Util.setLogLevel(GSet.class, Level.OFF);
    Util.setLogLevel(BlockManager.class, Level.OFF);
    Util.setLogLevel(DatanodeManager.class, Level.OFF);
    Util.setLogLevel(TopMetrics.class, Level.OFF);
  }

  static class Util {
    static String memoryInfo() {
      final Runtime runtime = Runtime.getRuntime();
      return "Memory Info: free=" + StringUtils.byteDesc(runtime.freeMemory())
          + ", total=" + StringUtils.byteDesc(runtime.totalMemory())
          + ", max=" + StringUtils.byteDesc(runtime.maxMemory());
    }

    static void setLogLevel(Class<?> clazz, Level level) {
      final Log log = LogFactory.getLog(clazz);
      if (log instanceof Log4JLogger) {
        final org.apache.log4j.Logger logger = ((Log4JLogger) log).getLogger();
        logger.setLevel(level);
        LOG.info("setLogLevel {} to {}, getEffectiveLevel() = {}",
            clazz.getName(), level, logger.getEffectiveLevel());
      } else {
        LOG.warn("Failed setLogLevel {} to {}", clazz.getName(), level);
      }
    }

    static String toCommaSeparatedNumber(long n) {
      final StringBuilder b = new StringBuilder();
      for(; n > 999;) {
        b.insert(0, String.format(",%03d", n%1000));
        n /= 1000;
      }
      return b.insert(0, n).toString();
    }
  }

  private final File fsImageFile;

  FsImageValidation(File fsImageFile) {
    this.fsImageFile = fsImageFile;
  }

  int checkINodeReference(Configuration conf) throws Exception {
    LOG.info(Util.memoryInfo());
    initConf(conf);

    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        final double percent = NameNode.getStartupProgress().createView()
            .getPercentComplete(Phase.LOADING_FSIMAGE);
        LOG.info(String.format("%s Progress: %.1f%%",
            Phase.LOADING_FSIMAGE, 100*percent));
      }
    };

    INodeReferenceValidation.start();
    final Timer t = new Timer();
    t.scheduleAtFixedRate(checkProgress, 0, 60_000);
    final long loadStart = now();
    final FSNamesystem namesystem;
    if (fsImageFile.isDirectory()) {
      Cli.println("Loading %s as a directory.", fsImageFile);
      final String dir = fsImageFile.getCanonicalPath();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dir);
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, dir);


      final FSImage fsImage = new FSImage(conf);
      namesystem = new FSNamesystem(conf, fsImage, true);
      // Avoid saving fsimage
      namesystem.setRollingUpgradeInfo(false, 0);

      namesystem.loadFSImage(HdfsServerConstants.StartupOption.REGULAR);
    } else {
      Cli.println("Loading %s as a file.", fsImageFile);
      final FSImage fsImage = new FSImage(conf);
      namesystem = new FSNamesystem(conf, fsImage, true);

      final NamespaceInfo namespaceInfo = NNStorage.newNamespaceInfo();
      namespaceInfo.clusterID = "cluster0";
      fsImage.getStorage().setStorageInfo(namespaceInfo);

      final FSImageFormat.LoaderDelegator loader
          = FSImageFormat.newLoader(conf, namesystem);
      namesystem.writeLock();
      namesystem.getFSDirectory().writeLock();
      try {
        loader.load(fsImageFile, false);
      } finally {
        namesystem.getFSDirectory().writeUnlock();
        namesystem.writeUnlock();
      }
    }
    t.cancel();
    Cli.println("Loaded %s %s successfully in %s",
        FS_IMAGE, fsImageFile, StringUtils.formatTime(now() - loadStart));
    LOG.info(Util.memoryInfo());
    final int errorCount = INodeReferenceValidation.end();
    LOG.info(Util.memoryInfo());
    return errorCount;
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
      initLogLevels();

      final FsImageValidation validation = FsImageValidation.newInstance(args);
      final int errorCount = validation.checkINodeReference(getConf());
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
      LOG.info(s);
    }

    static void printError(String message, Throwable t) {
      System.out.println(message);
      if (t != null) {
        t.printStackTrace(System.out);
      }
      LOG.error(message, t);
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, Cli.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new Configuration(), new Cli(), args));
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
