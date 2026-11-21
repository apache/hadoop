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
    xmlFilename = "hdfs-default.xml";
    configurationClasses = new Class[] { HdfsClientConfigKeys.class,
        HdfsClientConfigKeys.Failover.class,
        HdfsClientConfigKeys.StripedRead.class, DFSConfigKeys.class,
        HdfsClientConfigKeys.BlockWrite.class, HdfsClientConfigKeys.Write.class,
        HdfsClientConfigKeys.Read.class, HdfsClientConfigKeys.HedgedRead.class,
        HdfsClientConfigKeys.ShortCircuit.class,
        HdfsClientConfigKeys.Retry.class, HdfsClientConfigKeys.Mmap.class,
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.class,
        HdfsClientConfigKeys.Write.ECRedundancy.class};

    // Set error modes
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;

    // Initialize used variables
    configurationPropsToSkipCompare = new HashSet<String>();

    // Ignore testing based parameter
    configurationPropsToSkipCompare.add("ignore.secure.ports.for.testing");

    // Remove deprecated properties listed in Configuration#DeprecationDelta
    configurationPropsToSkipCompare.add(DFSConfigKeys.DFS_DF_INTERVAL_KEY);

    // Remove support property
    configurationPropsToSkipCompare
        .add(DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY);
    configurationPropsToSkipCompare
        .add(DFSConfigKeys.DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY);

    // Purposely hidden, based on comments in DFSConfigKeys
    configurationPropsToSkipCompare
        .add(DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY);

    // dfs.corruptfilesreturned.max is read into BlockManager but the
    // resulting field is never consulted; it has no effect. The live
    // equivalent is dfs.namenode.max-corrupt-file-blocks-returned.
    configurationPropsToSkipCompare
        .add("dfs.corruptfilesreturned.max");

    // Example (not real) property in hdfs-default.xml
    configurationPropsToSkipCompare.add("dfs.ha.namenodes");

    // Property used for internal testing only
    configurationPropsToSkipCompare
        .add(DFSConfigKeys.DFS_DATANODE_DUPLICATE_REPLICA_DELETION);

    // Property not intended for users
    configurationPropsToSkipCompare
        .add(DFSConfigKeys.DFS_DATANODE_STARTUP_KEY);
    configurationPropsToSkipCompare
        .add(DFSConfigKeys.DFS_NAMENODE_STARTUP_KEY);
    configurationPropsToSkipCompare.add(DFSConfigKeys
        .DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY);

    // Allocate
    xmlPropsToSkipCompare = new HashSet<String>();
    xmlPrefixToSkipCompare = new HashSet<String>();

    // Used in native code fuse_connect.c
    xmlPropsToSkipCompare.add("hadoop.fuse.timer.period");
    xmlPropsToSkipCompare.add("hadoop.fuse.connection.timeout");

    // Used dynamically as part of DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX
    xmlPropsToSkipCompare.add("dfs.namenode.edits.journal-plugin.qjournal");

    // Defined in org.apache.hadoop.fs.CommonConfigurationKeys
    xmlPropsToSkipCompare.add("hadoop.user.group.metrics.percentiles.intervals");

    // Used oddly by DataNode to create new config String
    xmlPropsToSkipCompare.add("hadoop.hdfs.configuration.version");

    // Some properties have moved to HdfsClientConfigKeys
    xmlPropsToSkipCompare.add("dfs.client.short.circuit.replica.stale.threshold.ms");

    // Ignore HTrace properties
    xmlPropsToSkipCompare.add("fs.client.htrace");
    xmlPropsToSkipCompare.add("hadoop.htrace");

    // Ignore SpanReceiveHost properties
    xmlPropsToSkipCompare.add("dfs.htrace.spanreceiver.classes");
    xmlPropsToSkipCompare.add("dfs.client.htrace.spanreceiver.classes");

    // Remove deprecated properties listed in Configuration#DeprecationDelta
    xmlPropsToSkipCompare.add(DFSConfigKeys.DFS_DF_INTERVAL_KEY);

    // Kept in the NfsConfiguration class in the hadoop-hdfs-nfs module
    xmlPrefixToSkipCompare.add("nfs");

    // Kept in BlackListBasedTrustedChannelResolver and
    // WhitelistBasedTrustedChannelResolver, not in DFSConfigKeys
    xmlPrefixToSkipCompare.add("dfs.datatransfer.server");
    xmlPrefixToSkipCompare.add("dfs.datatransfer.client");

    // Kept in EditLogTailer, not in DFSConfigKeys
    xmlPropsToSkipCompare.add("dfs.ha.tail-edits.max-txns-per-lock");

    // Kept in CredentialBasedAccessTokenProvider and
    // ConfRefreshTokenBasedAccessTokenProvider, not in HdfsClientConfigKeys
    xmlPropsToSkipCompare.add("dfs.webhdfs.oauth2.credential");
    xmlPropsToSkipCompare.add("dfs.webhdfs.oauth2.refresh.token");
    xmlPropsToSkipCompare
        .add("dfs.webhdfs.oauth2.refresh.token.expires.ms.since.epoch");

    // Kept in HdfsClientConfigKeys.Read.ShortCircuit, which is not in
    // configurationClasses and has no mirror constant in DFSConfigKeys
    xmlPropsToSkipCompare
        .add("dfs.client.read.shortcircuit.metrics.sampling.percentage");

    // Not a hardcoded property.  Used by SaslRpcClient
    xmlPrefixToSkipCompare.add("dfs.namenode.kerberos.principal.pattern");

    // Skip over example property
    xmlPrefixToSkipCompare.add("dfs.ha.namenodes");
  }
}
