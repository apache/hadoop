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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class provides rudimentary checking of DFS volumes for errors and
 * sub-optimal conditions.
 * <p>The tool scans all files and directories, starting from an indicated
 *  root path. The following abnormal conditions are detected and handled:</p>
 * <ul>
 * <li>files with blocks that are completely missing from all datanodes.<br/>
 * In this case the tool can perform one of the following actions:
 *  <ul>
 *      <li>none ({@link org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_NONE})</li>
 *      <li>move corrupted files to /lost+found directory on DFS
 *      ({@link org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_MOVE}). Remaining data blocks are saved as a
 *      block chains, representing longest consecutive series of valid blocks.</li>
 *      <li>delete corrupted files ({@link org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_DELETE})</li>
 *  </ul>
 *  </li>
 *  <li>detect files with under-replicated or over-replicated blocks</li>
 *  </ul>
 *  Additionally, the tool collects a detailed overall DFS statistics, and
 *  optionally can print detailed statistics on block locations and replication
 *  factors of each file.
 *  The tool also provides and option to filter open files during the scan.
 *  
 */
public class DFSck extends Configured implements Tool {

  DFSck() {}
  
  /**
   * Filesystem checker.
   * @param conf current Configuration
   * @throws Exception
   */
  public DFSck(Configuration conf) throws Exception {
    super(conf);
  }
  
  private String getInfoServer() throws IOException {
    return NetUtils.getServerAddress(getConf(), "dfs.info.bindAddress", 
                                     "dfs.info.port", "dfs.http.address");
  }
  
  /**
   * Print fsck usage information
   */
  static void printUsage() {
    System.err.println("Usage: DFSck <path> [-move | -delete | -openforwrite] [-files [-blocks [-locations | -racks]]]");
    System.err.println("\t<path>\tstart checking from this path");
    System.err.println("\t-move\tmove corrupted files to /lost+found");
    System.err.println("\t-delete\tdelete corrupted files");
    System.err.println("\t-files\tprint out files being checked");
    System.err.println("\t-openforwrite\tprint out files opened for write");
    System.err.println("\t-blocks\tprint out block report");
    System.err.println("\t-locations\tprint out locations for every block");
    System.err.println("\t-racks\tprint out network topology for data-node locations");
    System.err.println("\t\tBy default fsck ignores files opened for write, " +
                       "use -openforwrite to report such files. They are usually " +
                       " tagged CORRUPT or HEALTHY depending on their block " +
                        "allocation status");
    ToolRunner.printGenericCommandUsage(System.err);
  }
  /**
   * @param args
   */
  public int run(String[] args) throws Exception {
    String fsName = getInfoServer();
    if (args.length == 0) {
      printUsage();
      return -1;
    }
    StringBuffer url = new StringBuffer("http://"+fsName+"/fsck?path=");
    String dir = "/";
    // find top-level dir first
    for (int idx = 0; idx < args.length; idx++) {
      if (!args[idx].startsWith("-")) { dir = args[idx]; break; }
    }
    url.append(URLEncoder.encode(dir, "UTF-8"));
    for (int idx = 0; idx < args.length; idx++) {
      if (args[idx].equals("-move")) { url.append("&move=1"); }
      else if (args[idx].equals("-delete")) { url.append("&delete=1"); }
      else if (args[idx].equals("-files")) { url.append("&files=1"); }
      else if (args[idx].equals("-openforwrite")) { url.append("&openforwrite=1"); }
      else if (args[idx].equals("-blocks")) { url.append("&blocks=1"); }
      else if (args[idx].equals("-locations")) { url.append("&locations=1"); }
      else if (args[idx].equals("-racks")) { url.append("&racks=1"); }
    }
    URL path = new URL(url.toString());
    URLConnection connection = path.openConnection();
    InputStream stream = connection.getInputStream();
    BufferedReader input = new BufferedReader(new InputStreamReader(
                                              stream, "UTF-8"));
    String line = null;
    String lastLine = null;
    int errCode = -1;
    try {
      while ((line = input.readLine()) != null) {
        System.out.println(line);
        lastLine = line;
      }
    } finally {
      input.close();
    }
    if (lastLine.endsWith(NamenodeFsck.HEALTHY_STATUS)) {
      errCode = 0;
    } else if (lastLine.endsWith(NamenodeFsck.CORRUPT_STATUS)) {
      errCode = 1;
    } else if (lastLine.endsWith(NamenodeFsck.NONEXISTENT_STATUS)) {
      errCode = 0;
    }
    return errCode;
  }

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  
  public static void main(String[] args) throws Exception {
    // -files option is also used by GenericOptionsParser
    // Make sure that is not the first argument for fsck
    int res = -1;
    if ((args.length == 0 ) || ("-files".equals(args[0]))) 
      printUsage();
    else
      res = ToolRunner.run(new DFSck(new Configuration()), args);
    System.exit(res);
  }
}
