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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.hdfs.server.namenode.visitor.INodeCountVisitor;
import org.apache.hadoop.hdfs.server.namenode.visitor.INodeCountVisitor.Counts;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Cli.println;
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

  static String getEnv(String property) {
    final String value = System.getenv().get(property);
    LOG.info("ENV: {} = {}", property, value);
    return value;
  }

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
      final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(clazz);
      logger.setLevel(level);
      LOG.info("setLogLevel {} to {}, getEffectiveLevel() = {}", clazz.getName(), level,
          logger.getEffectiveLevel());
    }

    static String toCommaSeparatedNumber(long n) {
      final StringBuilder b = new StringBuilder();
      for(; n > 999;) {
        b.insert(0, String.format(",%03d", n%1000));
        n /= 1000;
      }
      return b.insert(0, n).toString();
    }

    /** @return a filter for the given type. */
    static FilenameFilter newFilenameFilter(NameNodeFile type) {
      final String prefix = type.getName() + "_";
      return new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (!name.startsWith(prefix)) {
            return false;
          }
          for (int i = prefix.length(); i < name.length(); i++) {
            if (!Character.isDigit(name.charAt(i))) {
              return false;
            }
          }
          return true;
        }
      };
    }
  }

  private final File fsImageFile;

  FsImageValidation(File fsImageFile) {
    this.fsImageFile = fsImageFile;
  }

  int run() throws Exception {
    return run(new Configuration(), new AtomicInteger());
  }

  int run(AtomicInteger errorCount) throws Exception {
    return run(new Configuration(), errorCount);
  }

  int run(Configuration conf, AtomicInteger errorCount) throws Exception {
    final int initCount = errorCount.get();
    LOG.info(Util.memoryInfo());
    initConf(conf);

    // check INodeReference
    final FSNamesystem namesystem = checkINodeReference(conf, errorCount);

    // check INodeMap
    INodeMapValidation.run(namesystem.getFSDirectory(), errorCount);
    LOG.info(Util.memoryInfo());

    final int d = errorCount.get() - initCount;
    if (d > 0) {
      Cli.println("Found %d error(s) in %s", d, fsImageFile.getAbsolutePath());
    }
    return d;
  }

  private FSNamesystem loadImage(Configuration conf) throws IOException {
    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        final double percent = NameNode.getStartupProgress().createView()
            .getPercentComplete(Phase.LOADING_FSIMAGE);
        LOG.info(String.format("%s Progress: %.1f%% (%s)",
            Phase.LOADING_FSIMAGE, 100*percent, Util.memoryInfo()));
      }
    };

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
        namesystem.writeUnlock("loadImage");
      }
    }
    t.cancel();
    Cli.println("Loaded %s %s successfully in %s",
        FS_IMAGE, fsImageFile, StringUtils.formatTime(now() - loadStart));
    return namesystem;
  }

  FSNamesystem checkINodeReference(Configuration conf,
      AtomicInteger errorCount) throws Exception {
    INodeReferenceValidation.start();
    final FSNamesystem namesystem = loadImage(conf);
    LOG.info(Util.memoryInfo());
    INodeReferenceValidation.end(errorCount);
    LOG.info(Util.memoryInfo());
    return namesystem;
  }

  static class INodeMapValidation {
    static Iterable<INodeWithAdditionalFields> iterate(INodeMap map) {
      return new Iterable<INodeWithAdditionalFields>() {
        @Override
        public Iterator<INodeWithAdditionalFields> iterator() {
          return map.getMapIterator();
        }
      };
    }

    static void run(FSDirectory fsdir, AtomicInteger errorCount) {
      final int initErrorCount = errorCount.get();
      final Counts counts = INodeCountVisitor.countTree(fsdir.getRoot());
      for (INodeWithAdditionalFields i : iterate(fsdir.getINodeMap())) {
        if (counts.getCount(i) == 0) {
          Cli.printError(errorCount, "%s (%d) is inaccessible (%s)",
              i, i.getId(), i.getFullPathName());
        }
      }
      println("%s ended successfully: %d error(s) found.",
          INodeMapValidation.class.getSimpleName(),
          errorCount.get() - initErrorCount);
    }
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
      final AtomicInteger errorCount = new AtomicInteger();
      validation.run(getConf(), errorCount);
      println("Error Count: %s", errorCount);
      return errorCount.get() == 0? 0: 1;
    }

    static String parse(String... args) {
      final String f;
      if (args == null || args.length == 0) {
        f = getEnv(FS_IMAGE);
      } else if (args.length == 1) {
        f = args[0];
      } else {
        throw new HadoopIllegalArgumentException(
            "args = " + Arrays.toString(args));
      }

      println("%s = %s", FS_IMAGE, f);
      return f;
    }

    static synchronized void println(String format, Object... args) {
      final String s = String.format(format, args);
      System.out.println(s);
      LOG.info(s);
    }

    static synchronized void warn(String format, Object... args) {
      final String s = "WARN: " + String.format(format, args);
      System.out.println(s);
      LOG.warn(s);
    }

    static synchronized void printError(String message, Throwable t) {
      System.out.println(message);
      if (t != null) {
        t.printStackTrace(System.out);
      }
      LOG.error(message, t);
    }

    static synchronized void printError(AtomicInteger errorCount,
        String format, Object... args) {
      final int count = errorCount.incrementAndGet();
      final String s = "FSIMAGE_ERROR " + count + ": "
          + String.format(format, args);
      System.out.println(s);
      LOG.info(s);
    }
  }

  public static int validate(FSNamesystem namesystem) throws Exception {
    final AtomicInteger errorCount = new AtomicInteger();
    final NNStorage nnStorage = namesystem.getFSImage().getStorage();
    for(Storage.StorageDirectory sd : nnStorage.getStorageDirs()) {
      validate(sd.getCurrentDir(), errorCount);
    }
    return errorCount.get();
  }

  public static void validate(File path, AtomicInteger errorCount)
      throws Exception {
    if (path.isFile()) {
      new FsImageValidation(path).run(errorCount);
    } else if (path.isDirectory()) {
      final File[] images = path.listFiles(
          Util.newFilenameFilter(NameNodeFile.IMAGE));
      if (images == null || images.length == 0) {
        Cli.warn("%s not found in %s", FSImage.class.getSimpleName(),
            path.getAbsolutePath());
        return;
      }

      Arrays.sort(images, Collections.reverseOrder());
      for (int i = 0; i < images.length; i++) {
        final File image = images[i];
        Cli.println("%s %d) %s", FSImage.class.getSimpleName(),
            i, image.getAbsolutePath());
        FsImageValidation.validate(image, errorCount);
      }
    }

    Cli.warn("%s is neither a file nor a directory", path.getAbsolutePath());
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
