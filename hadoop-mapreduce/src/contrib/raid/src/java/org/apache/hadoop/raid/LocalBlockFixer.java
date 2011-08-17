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

package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.RaidDFSUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.net.NetUtils;

import org.apache.hadoop.hdfs.DistributedFileSystem;

import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.raid.protocol.PolicyInfo.ErasureCodeType;

/**
 * This class fixes source file blocks using the parity file,
 * and parity file blocks using the source file.
 * It periodically fetches the list of corrupt files from the namenode,
 * and figures out the location of the bad block by reading through
 * the corrupt file.
 */
public class LocalBlockFixer extends BlockFixer {
  public static final Log LOG = LogFactory.getLog(LocalBlockFixer.class);

  private java.util.HashMap<String, java.util.Date> history;
  
  private BlockFixerHelper helper;

  public LocalBlockFixer(Configuration conf) throws IOException {
    super(conf);
    history = new java.util.HashMap<String, java.util.Date>();
    helper = new BlockFixerHelper(getConf());
  }

  public void run() {
    while (running) {
      try {
        LOG.info("LocalBlockFixer continuing to run...");
        doFix();
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
      } catch (Error err) {
        LOG.error("Exiting after encountering " +
                    StringUtils.stringifyException(err));
        throw err;
      }
    }
  }

  void doFix() throws InterruptedException, IOException {
    while (running) {
      // Sleep before proceeding to fix files.
      Thread.sleep(blockFixInterval);

      // Purge history older than the history interval.
      purgeHistory();

      List<Path> corruptFiles = getCorruptFiles();

      filterUnfixableSourceFiles(corruptFiles.iterator());

      if (corruptFiles.isEmpty()) {
        // If there are no corrupt files, retry after some time.
        continue;
      }
      LOG.info("Found " + corruptFiles.size() + " corrupt files.");

      helper.sortCorruptFiles(corruptFiles);

      for (Path srcPath: corruptFiles) {
        if (!running) break;
        try {
          boolean fixed = helper.fixFile(srcPath);
          LOG.info("Adding " + srcPath + " to history");
          history.put(srcPath.toString(), new java.util.Date());
          if (fixed) {
            incrFilesFixed();
          }
        } catch (IOException ie) {
          LOG.error("Hit error while processing " + srcPath +
            ": " + StringUtils.stringifyException(ie));
          // Do nothing, move on to the next file.
        }
      }
    }
  }


  /**
   * We maintain history of fixed files because a fixed file may appear in
   * the list of corrupt files if we loop around too quickly.
   * This function removes the old items in the history so that we can
   * recognize files that have actually become corrupt since being fixed.
   */
  void purgeHistory() {
    java.util.Date cutOff = new java.util.Date(System.currentTimeMillis() -
                                               historyInterval);
    List<String> toRemove = new java.util.ArrayList<String>();

    for (String key: history.keySet()) {
      java.util.Date item = history.get(key);
      if (item.before(cutOff)) {
        toRemove.add(key);
      }
    }
    for (String key: toRemove) {
      LOG.info("Removing " + key + " from history");
      history.remove(key);
    }
  }

  /**
   * @return A list of corrupt files as obtained from the namenode
   */
  List<Path> getCorruptFiles() throws IOException {
    DistributedFileSystem dfs = helper.getDFS(new Path("/"));

    String[] files = RaidDFSUtil.getCorruptFiles(dfs);
    List<Path> corruptFiles = new LinkedList<Path>();
    for (String f: files) {
      Path p = new Path(f);
      if (!history.containsKey(p.toString())) {
        corruptFiles.add(p);
      }
    }
    RaidUtils.filterTrash(getConf(), corruptFiles);
    return corruptFiles;
  }

}

