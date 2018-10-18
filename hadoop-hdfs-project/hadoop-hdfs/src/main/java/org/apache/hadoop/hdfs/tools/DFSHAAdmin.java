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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Class to extend HAAdmin to do a little bit of HDFS-specific configuration.
 */
public class DFSHAAdmin extends HAAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(DFSHAAdmin.class);

  private String nameserviceId;

  protected void setErrOut(PrintStream errOut) {
    this.errOut = errOut;
  }
  
  protected void setOut(PrintStream out) {
    this.out = out;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }

  /**
   * Add the requisite security principal settings to the given Configuration,
   * returning a copy.
   * @param conf the original config
   * @return a copy with the security settings added
   */
  public static Configuration addSecurityConfiguration(Configuration conf) {
    // Make a copy so we don't mutate it. Also use an HdfsConfiguration to
    // force loading of hdfs-site.xml.
    conf = new HdfsConfiguration(conf);
    String nameNodePrincipal = conf.get(
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using NN principal: " + nameNodePrincipal);
    }

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        nameNodePrincipal);
    return conf;
  }

  /**
   * Try to map the given namenode ID to its service address.
   */
  @Override
  protected HAServiceTarget resolveTarget(String nnId) {
    HdfsConfiguration conf = (HdfsConfiguration)getConf();
    return new NNHAServiceTarget(conf, nameserviceId, nnId);
  }

  @Override
  protected String getUsageString() {
    return "Usage: haadmin [-ns <nameserviceId>]";
  }

  @Override
  protected int runCmd(String[] argv) throws Exception {
    if (argv.length < 1) {
      printUsage(errOut);
      return -1;
    }

    int i = 0;
    String cmd = argv[i++];

    if ("-ns".equals(cmd)) {
      if (i == argv.length) {
        errOut.println("Missing nameservice ID");
        printUsage(errOut);
        return -1;
      }
      nameserviceId = argv[i++];
      if (i >= argv.length) {
        errOut.println("Missing command");
        printUsage(errOut);
        return -1;
      }
      argv = Arrays.copyOfRange(argv, i, argv.length);
    }

    return super.runCmd(argv);
  }
  
  /**
   * returns the list of all namenode ids for the given configuration 
   */
  @Override
  protected Collection<String> getTargetIds(String namenodeToActivate) {
    return DFSUtilClient.getNameNodeIds(getConf(),
                                        (nameserviceId != null) ? nameserviceId : DFSUtil.getNamenodeNameServiceId(
                                            getConf()));
  }
  
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new DFSHAAdmin(), argv);
    System.exit(res);
  }
}
