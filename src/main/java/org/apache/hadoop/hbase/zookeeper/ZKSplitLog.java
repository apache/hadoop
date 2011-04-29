/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Common methods and attributes used by {@link SplitLogManager} and
 * {@link SplitLogWorker}
 */
public class ZKSplitLog {
  private static final Log LOG = LogFactory.getLog(ZKSplitLog.class);

  public static final int DEFAULT_TIMEOUT = 25000; // 25 sec
  public static final int DEFAULT_ZK_RETRIES = 3;
  public static final int DEFAULT_MAX_RESUBMIT = 3;
  public static final int DEFAULT_UNASSIGNED_TIMEOUT = (3 * 60 * 1000); //3 min

  /**
   * Gets the full path node name for the log file being split
   * @param zkw zk reference
   * @param filename log file name (only the basename)
   */
  public static String getEncodedNodeName(ZooKeeperWatcher zkw,
      String filename) {
      return ZKUtil.joinZNode(zkw.splitLogZNode, encode(filename));
  }

  public static String getFileName(String node) {
    String basename = node.substring(node.lastIndexOf('/') + 1);
    return decode(basename);
  }


  public static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("URLENCODER doesn't support UTF-8");
    }
  }

  public static String decode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("URLDecoder doesn't support UTF-8");
    }
  }

  public static String getRescanNode(ZooKeeperWatcher zkw) {
    return ZKUtil.joinZNode(zkw.splitLogZNode, "RESCAN");
  }

  public static boolean isRescanNode(ZooKeeperWatcher zkw, String path) {
    String prefix = getRescanNode(zkw);
    if (path.length() <= prefix.length()) {
      return false;
    }
    for (int i = 0; i < prefix.length(); i++) {
      if (prefix.charAt(i) != path.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isTaskPath(ZooKeeperWatcher zkw, String path) {
    String dirname = path.substring(0, path.lastIndexOf('/'));
    return dirname.equals(zkw.splitLogZNode);
  }

  public static enum TaskState {
    TASK_UNASSIGNED("unassigned"),
    TASK_OWNED("owned"),
    TASK_RESIGNED("resigned"),
    TASK_DONE("done"),
    TASK_ERR("err");

    private final byte[] state;
    private TaskState(String s) {
      state = s.getBytes();
    }

    public byte[] get(String serverName) {
      return (Bytes.add(state, " ".getBytes(), serverName.getBytes()));
    }

    public String getWriterName(byte[] data) {
      String str = Bytes.toString(data);
      return str.substring(str.indexOf(' ') + 1);
    }


    /**
     * @param s
     * @return True if {@link #state} is a prefix of s. False otherwise.
     */
    public boolean equals(byte[] s) {
      if (s.length < state.length) {
        return (false);
      }
      for (int i = 0; i < state.length; i++) {
        if (state[i] != s[i]) {
          return (false);
        }
      }
      return (true);
    }

    public boolean equals(byte[] s, String serverName) {
      return (Arrays.equals(s, get(serverName)));
    }
    @Override
    public String toString() {
      return new String(state);
    }
  }

  public static Path getSplitLogDir(Path rootdir, String tmpname) {
    return new Path(new Path(rootdir, "splitlog"), tmpname);
  }

  public static Path stripSplitLogTempDir(Path rootdir, Path file) {
    int skipDepth = rootdir.depth() + 2;
    List<String> components = new ArrayList<String>(10);
    do {
      components.add(file.getName());
      file = file.getParent();
    } while (file.depth() > skipDepth);
    Path ret = rootdir;
    for (int i = components.size() - 1; i >= 0; i--) {
      ret = new Path(ret, components.get(i));
    }
    return ret;
  }

  public static String getSplitLogDirTmpComponent(String worker, String file) {
    return (worker + "_" + ZKSplitLog.encode(file));
  }

  public static void markCorrupted(Path rootdir, String tmpname,
      FileSystem fs) {
    Path file = new Path(getSplitLogDir(rootdir, tmpname), "corrupt");
    try {
      fs.createNewFile(file);
    } catch (IOException e) {
      LOG.warn("Could not flag a log file as corrupted. Failed to create " +
          file, e);
    }
  }

  public static boolean isCorrupted(Path rootdir, String tmpname,
      FileSystem fs) throws IOException {
    Path file = new Path(getSplitLogDir(rootdir, tmpname), "corrupt");
    boolean isCorrupt;
    isCorrupt = fs.exists(file);
    return isCorrupt;
  }

  public static boolean isCorruptFlagFile(Path file) {
    return file.getName().equals("corrupt");
  }


  public static class Counters {
    //SplitLogManager counters
    public static AtomicLong tot_mgr_log_split_batch_start = new AtomicLong(0);
    public static AtomicLong tot_mgr_log_split_batch_success =
      new AtomicLong(0);
    public static AtomicLong tot_mgr_log_split_batch_err = new AtomicLong(0);
    public static AtomicLong tot_mgr_new_unexpected_hlogs = new AtomicLong(0);
    public static AtomicLong tot_mgr_log_split_start = new AtomicLong(0);
    public static AtomicLong tot_mgr_log_split_success = new AtomicLong(0);
    public static AtomicLong tot_mgr_log_split_err = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_create_queued = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_create_result = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_already_exists = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_create_err = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_create_retry = new AtomicLong(0);
    public static AtomicLong tot_mgr_get_data_queued = new AtomicLong(0);
    public static AtomicLong tot_mgr_get_data_result = new AtomicLong(0);
    public static AtomicLong tot_mgr_get_data_err = new AtomicLong(0);
    public static AtomicLong tot_mgr_get_data_retry = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_delete_queued = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_delete_result = new AtomicLong(0);
    public static AtomicLong tot_mgr_node_delete_err = new AtomicLong(0);
    public static AtomicLong tot_mgr_resubmit = new AtomicLong(0);
    public static AtomicLong tot_mgr_resubmit_failed = new AtomicLong(0);
    public static AtomicLong tot_mgr_null_data = new AtomicLong(0);
    public static AtomicLong tot_mgr_orphan_task_acquired = new AtomicLong(0);
    public static AtomicLong tot_mgr_unacquired_orphan_done = new AtomicLong(0);
    public static AtomicLong tot_mgr_resubmit_threshold_reached =
      new AtomicLong(0);
    public static AtomicLong tot_mgr_missing_state_in_delete =
      new AtomicLong(0);
    public static AtomicLong tot_mgr_heartbeat = new AtomicLong(0);
    public static AtomicLong tot_mgr_rescan = new AtomicLong(0);
    public static AtomicLong tot_mgr_rescan_deleted = new AtomicLong(0);
    public static AtomicLong tot_mgr_task_deleted = new AtomicLong(0);
    public static AtomicLong tot_mgr_resubmit_unassigned = new AtomicLong(0);
    public static AtomicLong tot_mgr_relist_logdir = new AtomicLong(0);



    // SplitLogWorker counters
    public static AtomicLong tot_wkr_failed_to_grab_task_no_data =
      new AtomicLong(0);
    public static AtomicLong tot_wkr_failed_to_grab_task_exception =
      new AtomicLong(0);
    public static AtomicLong tot_wkr_failed_to_grab_task_owned =
      new AtomicLong(0);
    public static AtomicLong tot_wkr_failed_to_grab_task_lost_race =
      new AtomicLong(0);
    public static AtomicLong tot_wkr_task_acquired = new AtomicLong(0);
    public static AtomicLong tot_wkr_task_resigned = new AtomicLong(0);
    public static AtomicLong tot_wkr_task_done = new AtomicLong(0);
    public static AtomicLong tot_wkr_task_err = new AtomicLong(0);
    public static AtomicLong tot_wkr_task_heartbeat = new AtomicLong(0);
    public static AtomicLong tot_wkr_task_acquired_rescan = new AtomicLong(0);
    public static AtomicLong tot_wkr_get_data_queued = new AtomicLong(0);
    public static AtomicLong tot_wkr_get_data_result = new AtomicLong(0);
    public static AtomicLong tot_wkr_get_data_retry = new AtomicLong(0);
    public static AtomicLong tot_wkr_preempt_task = new AtomicLong(0);
    public static AtomicLong tot_wkr_task_heartbeat_failed = new AtomicLong(0);
    public static AtomicLong tot_wkr_final_transistion_failed =
      new AtomicLong(0);

    public static void resetCounters() throws Exception {
      Class<?> cl = (new Counters()).getClass();
      Field[] flds = cl.getDeclaredFields();
      for (Field fld : flds) {
        ((AtomicLong)fld.get(null)).set(0);
      }
    }
  }
}
