/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable {

  final static String NAME = "copytable";
  static String rsClass = null;
  static String rsImpl = null;
  static long startTime = 0;
  static long endTime = 0;
  static String tableName = null;
  static String newTableName = null;
  static String peerAddress = null;
  static String families = null;

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(CopyTable.class);
    Scan scan = new Scan();
    if (startTime != 0) {
      scan.setTimeRange(startTime,
          endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    }
    if(families != null) {
      String[] fams = families.split(",");
      Map<String,String> cfRenameMap = new HashMap<String,String>();
      for(String fam : fams) {
        String sourceCf;
        if(fam.contains(":")) { 
            // fam looks like "sourceCfName:destCfName"
            String[] srcAndDest = fam.split(":", 2);
            sourceCf = srcAndDest[0];
            String destCf = srcAndDest[1];
            cfRenameMap.put(sourceCf, destCf);
        } else {
            // fam is just "sourceCf"
            sourceCf = fam; 
        }
        scan.addFamily(Bytes.toBytes(sourceCf));
      }
      Import.configureCfRenaming(job.getConfiguration(), cfRenameMap);
    }
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        Import.Importer.class, null, null, job);
    TableMapReduceUtil.initTableReducerJob(
        newTableName == null ? tableName : newTableName, null, job,
        null, peerAddress, rsClass, rsImpl);
    job.setNumReduceTasks(0);
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: CopyTable [--rs.class=CLASS] " +
        "[--rs.impl=IMPL] [--starttime=X] [--endtime=Y] " +
        "[--new.name=NEW] [--peer.adr=ADR] <tablename>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" rs.class     hbase.regionserver.class of the peer cluster");
    System.err.println("              specify if different from current cluster");
    System.err.println(" rs.impl      hbase.regionserver.impl of the peer cluster");
    System.err.println(" starttime    beginning of the time range");
    System.err.println("              without endtime means from starttime to forever");
    System.err.println(" endtime      end of the time range");
    System.err.println(" new.name     new table's name");
    System.err.println(" peer.adr     Address of the peer cluster given in the format");
    System.err.println("              hbase.zookeeer.quorum:hbase.zookeeper.client.port:zookeeper.znode.parent");
    System.err.println(" families     comma-separated list of families to copy");
    System.err.println("              To copy from cf1 to cf2, give sourceCfName:destCfName. ");
    System.err.println("              To keep the same name, just give \"cfName\"");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" tablename    Name of the table to copy");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To copy 'TestTable' to a cluster that uses replication for a 1 hour window:");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.CopyTable --rs.class=org.apache.hadoop.hbase.ipc.ReplicationRegionInterface " +
        "--rs.impl=org.apache.hadoop.hbase.regionserver.replication.ReplicationRegionServer --starttime=1265875194289 --endtime=1265878794289 " +
        "--peer.adr=server1,server2,server3:2181:/hbase --families=myOldCf:myNewCf,cf2,cf3 TestTable ");
  }

  private static boolean doCommandLine(final String[] args) {
    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).
    if (args.length < 1) {
      printUsage(null);
      return false;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }

        final String rsClassArgKey = "--rs.class=";
        if (cmd.startsWith(rsClassArgKey)) {
          rsClass = cmd.substring(rsClassArgKey.length());
          continue;
        }

        final String rsImplArgKey = "--rs.impl=";
        if (cmd.startsWith(rsImplArgKey)) {
          rsImpl = cmd.substring(rsImplArgKey.length());
          continue;
        }

        final String startTimeArgKey = "--starttime=";
        if (cmd.startsWith(startTimeArgKey)) {
          startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
          continue;
        }

        final String endTimeArgKey = "--endtime=";
        if (cmd.startsWith(endTimeArgKey)) {
          endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
          continue;
        }

        final String newNameArgKey = "--new.name=";
        if (cmd.startsWith(newNameArgKey)) {
          newTableName = cmd.substring(newNameArgKey.length());
          continue;
        }

        final String peerAdrArgKey = "--peer.adr=";
        if (cmd.startsWith(peerAdrArgKey)) {
          peerAddress = cmd.substring(peerAdrArgKey.length());
          continue;
        }

        final String familiesArgKey = "--families=";
        if (cmd.startsWith(familiesArgKey)) {
          families = cmd.substring(familiesArgKey.length());
          continue;
        }

        if (i == args.length-1) {
          tableName = cmd;
        }
      }
      if (newTableName == null && peerAddress == null) {
        printUsage("At least a new table name or a " +
            "peer address must be specified");
        return false;
      }
    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Job job = createSubmittableJob(conf, args);
    if (job != null) {
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
}
