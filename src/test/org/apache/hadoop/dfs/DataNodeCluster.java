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
package org.apache.hadoop.dfs;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants.StartupOption;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;


/**
 * 

 * 
 * This program starts a mini cluster of data nodes
 *  (ie a mini cluster without the name node), all within one address space.
 *  It is assumed that the name node has been started separately prior
 *  to running this program.
 *  
 *  A use case of this is to run a real name node with a large number of
 *  simulated data nodes for say a NN benchmark.
 *  
 * Synopisis:
 *   DataNodeCluster -n numDatNodes [-r numRacks] -simulated
 *              [-inject startingBlockId numBlocksPerDN]
 *
 * if -simulated is specified then simulated data nodes are started.
 * if -inject is specified then blocks are injected in each datanode;
 *    -inject option is valid only for simulated data nodes.
 *    
 *    See Also @link #CreateEditsLog for creating a edits log file to
 *    inject a matching set of blocks into into a name node.
 *    Typical use of -inject is to inject blocks into a set of datanodes
 *    using this DataNodeCLuster command
 *    and then to inject the same blocks into a name node using the
 *    CreateEditsLog command.
 *
 */

public class DataNodeCluster {
  static String usage =
    "Usage: datanodecluster " +
    " -n <numDataNodes> " + 
    " [-r <numRacks>] " +
    " [-simulated] " +
    " [-inject startingBlockId numBlocksPerDN]" +
    "\n" + 
    "  If -r not specified then default rack is used for all Data Nodes\n" +
    "  Data nodes are simulated if -simulated OR conf file specifies simulated\n";
  
  
  static void printUsageExit() {
    System.out.println(usage);
    System.exit(-1); 
  }
  static void printUsageExit(String err) {
    System.out.println(err);
    printUsageExit();
  }
  
  public static void main(String[] args) {
    int numDataNodes = 0;
    int numRacks = 0;
    boolean inject = false;
    long startingBlockId = 1;
    int numBlocksPerDNtoInject = 0;
    
    Configuration conf = new Configuration();

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-n")) {
        if (++i >= args.length) {
          printUsageExit("missing number of nodes" + i + " " + args.length);
        }
        numDataNodes = Integer.parseInt(args[i]);
      } else if (args[i].equals("-r")) {
        if (++i >= args.length) {
          printUsageExit("Missing number of racks");
        }
        numRacks = Integer.parseInt(args[i]);
      } else if (args[i].equals("-simulated")) {
        conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
      } else if (args[i].equals("-inject")) {
        if (!conf.getBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED,
                                                                false) ) {
          System.out.print("-inject is valid only for simulated");
          printUsageExit(); 
        }
       inject = true;
       if (++i >= args.length) {
         printUsageExit(
             "Missing starting block and number of blocks per DN to inject");
       }
       startingBlockId = Integer.parseInt(args[i]);
       if (++i >= args.length) {
         printUsageExit("Missing number of blocks to inject");
       }
       numBlocksPerDNtoInject = Integer.parseInt(args[i]);      
      } else {
        printUsageExit();
      }
    }
    if (numDataNodes <= 0) {
      System.out.println(usage);
      System.exit(-1);
    }
    String nameNodeAdr = FileSystem.getDefaultUri(conf).getAuthority();
    if (nameNodeAdr == null) {
      System.out.println("No name node address and port in config");
      System.exit(-1);
    }
    boolean simulated = 
      conf.getBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, false);
    System.out.println("Starting " + numDataNodes + 
          (simulated ? "Simulated " : " ") +
          " Data Nodes that will connect to Name Node at " + nameNodeAdr);
  
    System.setProperty("test.build.data", "/tmp/DataNodeCluster");

    MiniDFSCluster mc = new MiniDFSCluster();
    try {
      mc.formatDataNodeDirs();
    } catch (IOException e) {
      System.out.println("Error formating data node dirs:" + e);
    }
    String[] racks = null;
    String[] rack4DataNode = null;
    if (numRacks > 0) {
      System.out.println("Using " + numRacks + " racks: ");
      String rackPrefix = getUniqueRackPrefix();

      rack4DataNode = new String[numDataNodes];
      for (int i = 0; i < numDataNodes; ++i ) {
        //rack4DataNode[i] = racks[i%numRacks];
        rack4DataNode[i] = rackPrefix + "-" + i%numRacks;
        System.out.println("Data Node " + i + " using " + rack4DataNode[i]);
        
        
      }
    }
    try {
      mc.startDataNodes(conf, numDataNodes, true, StartupOption.REGULAR,
          rack4DataNode);
      if (inject) {
        long blockSize = 10;
        System.out.println("Injecting " + numBlocksPerDNtoInject +
            " blocks in each DN starting at blockId " + startingBlockId +
            " with blocksize of " + blockSize);
        Block[] blocks = new Block[numBlocksPerDNtoInject];
        long blkid = startingBlockId;
        for (int i_dn = 0; i_dn < numDataNodes; ++i_dn) {
          for (int i = 0; i < blocks.length; ++i) {
            blocks[i] = new Block(blkid++, blockSize, 1);
          }
          mc.injectBlocks(i_dn, blocks);
        }
        System.out.println("Created blocks from Bids " 
            + startingBlockId + " to "  + (blkid -1));
      }

    } catch (IOException e) {
      System.out.println("Error creating data node:" + e);
    }  
  }

  /*
   * There is high probability that the rack id generated here will 
   * not conflict with those of other data node cluster.
   * Not perfect but mostly unique rack ids are good enough
   */
  static private String getUniqueRackPrefix() {
  
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException ignored) {
      System.out.println("Could not find ip address of \"default\" inteface.");
    }
    
    int rand = 0;
    try {
      rand = SecureRandom.getInstance("SHA1PRNG").nextInt(Integer.MAX_VALUE);
    } catch (NoSuchAlgorithmException e) {
      rand = (new Random()).nextInt(Integer.MAX_VALUE);
    }
    return "/Rack-" + rand + "-"+ ip  + "-" + 
                      System.currentTimeMillis();
  }
}
