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
package org.apache.hadoop.hbase.util.hbck;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.io.MultipleIOException;

/**
 * This code is used to rebuild meta off line from file system data. If there
 * are any problem detected, it will fail suggesting actions for the user to do
 * to "fix" problems. If it succeeds, it will backup the previous .META. and
 * -ROOT- dirs and write new tables in place.
 * 
 * This is an advanced feature, so is only exposed for use if explicitly
 * mentioned.
 * 
 * hbase org.apache.hadoop.hbase.util.hbck.OfflineMetaRepair ...
 */
public class OfflineMetaRepair {
  private static final Log LOG = LogFactory.getLog(HBaseFsck.class.getName());
  HBaseFsck fsck;

  protected static void printUsageAndExit() {
    System.err.println("Usage: OfflineMetaRepair [opts] ");
    System.err.println(" where [opts] are:");
    System.err
        .println("   -details          Display full report of all regions.");
    System.err.println("   -base <hdfs://>   Base Hbase Data directory");
    Runtime.getRuntime().exit(-2);
  }

  /**
   * Main program
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", conf.get(HConstants.HBASE_DIR));
    HBaseFsck fsck = new HBaseFsck(conf);

    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-details")) {
        fsck.displayFullReport();
      } else if (cmd.equals("-base")) {
        // update hbase root dir to user-specified base
        i++;
        String path = args[i];
        conf.set(HConstants.HBASE_DIR, path);
        conf.set("fs.defaultFS", conf.get(HConstants.HBASE_DIR));
      } else {
        String str = "Unknown command line option : " + cmd;
        LOG.info(str);
        System.out.println(str);
        printUsageAndExit();
      }
    }

    // Fsck doesn't shutdown and and doesn't provide a way to shutdown its
    // threads cleanly, so we do a System.exit.
    boolean success = false;
    try {
      success = fsck.rebuildMeta();
    } catch (MultipleIOException mioes) {
      for (IOException ioe : mioes.getExceptions()) {
        LOG.error("Bailed out due to:", ioe);
      }
    } catch (Exception e) {
      LOG.error("Bailed out due to: ", e);
    } finally {
      System.exit(success ? 0 : 1);
    }
  }
}
