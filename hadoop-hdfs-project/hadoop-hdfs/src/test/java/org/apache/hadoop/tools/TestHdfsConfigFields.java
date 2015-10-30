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

package org.apache.hadoop.tools;

import java.util.HashSet;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

/**
 * Unit test class to compare the following MR Configuration classes:
 * <p></p>
 * {@link org.apache.hadoop.hdfs.DFSConfigKeys}
 * <p></p>
 * against hdfs-default.xml for missing properties.  Currently only
 * throws an error if the class is missing a property.
 * <p></p>
 * Refer to {@link org.apache.hadoop.conf.TestConfigurationFieldsBase}
 * for how this class works.
 */
public class TestHdfsConfigFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String("hdfs-default.xml");
    configurationClasses = new Class[] { HdfsClientConfigKeys.class,
        DFSConfigKeys.class};

    // Set error modes
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = false;

    // Allocate
    xmlPropsToSkipCompare = new HashSet<String>();
    xmlPrefixToSkipCompare = new HashSet<String>();

    // Used in native code fuse_connect.c
    xmlPropsToSkipCompare.add("hadoop.fuse.timer.period");
    xmlPropsToSkipCompare.add("hadoop.fuse.connection.timeout");

    // Used dynamically as part of DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX
    xmlPropsToSkipCompare.add("dfs.namenode.edits.journal-plugin.qjournal");

    // Example (not real) property in hdfs-default.xml
    xmlPropsToSkipCompare.add("dfs.ha.namenodes.EXAMPLENAMESERVICE");

    // Defined in org.apache.hadoop.fs.CommonConfigurationKeys
    xmlPropsToSkipCompare.add("hadoop.user.group.metrics.percentiles.intervals");

    // Used oddly by DataNode to create new config String
    xmlPropsToSkipCompare.add("hadoop.hdfs.configuration.version");

    // Kept in the NfsConfiguration class in the hadoop-hdfs-nfs module
    xmlPrefixToSkipCompare.add("nfs");

    // Not a hardcoded property.  Used by SaslRpcClient
    xmlPrefixToSkipCompare.add("dfs.namenode.kerberos.principal.pattern");

    // Skip comparing in branch-2.  Removed in trunk with HDFS-7985.
    xmlPropsToSkipCompare.add("dfs.webhdfs.enabled");

    // Some properties have moved to HdfsClientConfigKeys
    xmlPropsToSkipCompare.add("dfs.client.short.circuit.replica.stale.threshold.ms");

    // Ignore HTrace properties
    xmlPropsToSkipCompare.add("fs.client.htrace");
    xmlPropsToSkipCompare.add("hadoop.htrace");
  }
}
