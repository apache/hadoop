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
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

/**
 * For validating {@link FSImage}.
 *
 * This tool will load the user specified {@link FSImage}
 * and then build the namespace tree in order to run the validation.
 *
 * The main difference of this tool and
 * {@link org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer}
 * is that
 * {@link org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer}
 * only loads {@link FSImage} but it does not build the namespace tree.
 */
public class FsImageValidation {
  static final Logger LOG = LoggerFactory.getLogger(FsImageValidation.class);

  static final String FS_IMAGE_FILE = "FS_IMAGE_FILE";

  static int checkINodeReference(File fsImageFile) throws Exception {
    LOG.info("Check INodeReference for {}: {}", FS_IMAGE_FILE, fsImageFile);

    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        final StartupProgressView view = NameNode.getStartupProgress().createView();
        final double percent = view.getPercentComplete(Phase.LOADING_FSIMAGE);
        LOG.info("{} Progress: {}%", Phase.LOADING_FSIMAGE,
            String.format("%.1f", 100 * percent));
      }
    };
    final Timer t = new Timer();
    t.scheduleAtFixedRate(checkProgress, 0, 60_000);

    final Configuration conf = new HdfsConfiguration();
    final FSImage fsImage = new FSImage(conf);
    final FSNamesystem namesystem = new FSNamesystem(conf, fsImage, false);

    final NamespaceInfo namespaceInfo = NNStorage.newNamespaceInfo();
    namespaceInfo.clusterID = "cluster0";
    fsImage.getStorage().setStorageInfo(namespaceInfo);

    final FSImageFormat.LoaderDelegator loader = FSImageFormat.newLoader(conf, namesystem);
    INodeReferenceValidation.start();
    namesystem.writeLock();
    namesystem.getFSDirectory().writeLock();
    try {
      loader.load(fsImageFile, false);
    } finally {
      namesystem.getFSDirectory().writeUnlock();
      namesystem.writeUnlock();
      t.cancel();
    }
    return INodeReferenceValidation.end();
  }

  static File getFsImageFile(String... args) {
    final String f = parse(args);
    if (f == null) {
      throw new HadoopIllegalArgumentException(
              FS_IMAGE_FILE + " is not specified.");
    }
    return new File(f);
  }

  static String parse(String... args) {
    final String f;
    if (args == null || args.length == 0) {
      f = System.getenv().get(FS_IMAGE_FILE);
      if (f != null) {
        LOG.info("Environment variable {} = {}", FS_IMAGE_FILE, f);
      }
    } else if (args.length == 1) {
      f = args[0];
    } else {
      throw new HadoopIllegalArgumentException(
              "args = " + Arrays.toString(args));
    }

    LOG.info("{} = {}", FS_IMAGE_FILE, f);
    return f;
  }

  static class Cli extends Configured implements Tool {
    static final String COMMAND;
    static final String USAGE;
    static {
      final String clazz = FsImageValidation.class.getSimpleName();
      COMMAND = Character.toLowerCase(clazz.charAt(0)) + clazz.substring(1);
      USAGE = "Usage: hdfs " + COMMAND + " <" + FS_IMAGE_FILE + ">";
    }

    @Override
    public int run(String[] args) throws Exception {
      final int errorCount = checkINodeReference(getFsImageFile(args));
      LOG.info("Error Count: {}", errorCount);
      return errorCount == 0? 0: 1;
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
      LOG.error("Failed to run " + Cli.COMMAND, e);
      System.exit(-2);
    }
  }
}
