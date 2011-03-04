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
package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.test.system.HDFSCluster;
import org.apache.hadoop.hdfs.test.system.NNClient;
import org.apache.hadoop.hdfs.test.system.DNClient;

import org.apache.hadoop.mapreduce.test.system.MRCluster;

import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.test.system.AbstractDaemonClient;
import org.apache.hadoop.util.Progressable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

public class TestBalancer {

    private static final Log LOG = LogFactory.getLog(TestBalancer.class);
    private static final String BALANCER_TEMP_DIR = "balancer-temp";
    private Configuration hadoopConf;
    private HDFSCluster dfsCluster;
    private MRCluster mrCluster;

    public TestBalancer() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        hadoopConf = new Configuration();
        dfsCluster = HDFSCluster.createCluster(hadoopConf);
        dfsCluster.setUp();
        //TODO no need for mr cluster anymore
        mrCluster = MRCluster.createCluster(hadoopConf);
        mrCluster.setUp();
        //connectJMX();
    }

    @After
    public void tearDown() throws Exception {
        dfsCluster.tearDown();
        mrCluster.tearDown();
    }

    // Trivial @Test
    public void testNamenodePing() throws IOException {
        LOG.info("testing filesystem ping");
        NNClient namenode = dfsCluster.getNNClient();
        namenode.ping();
        LOG.info("done.");
    }

    // Trivial @Test
    public void testNamenodeConnectDisconnect() throws IOException {
        LOG.info("connecting to namenode");
        NNClient namenode = dfsCluster.getNNClient();
        namenode.connect();
        LOG.info("done.");
        LOG.info("disconnecting from namenode");
        namenode.disconnect();
    }

    /**
     * The basic scenario for balancer test is as follows
     *
     *  - Bring up cluster with 1 DataNode
     *  - Load DataNode to >50%
     *  - Count files/blocks on DataNode
     *  - Add new, empty DataNode to cluster
     *  - Run Balancer
     *  - Count files/blocks on DataNodes
     *  - Blocks counts from before and after Balancer run should be consistent
     *
     */
    @Test
    public void testBalancerBasicScenario() throws IOException {
        List<DNClient> killDNList = null;
        List<DNClient> testDNList = null;
        Path balancerTempDir = null;
        try {
            DNClient[] datanodes = getReserveDatanodes();
            DNClient datanode1 = datanodes[0];
            DNClient datanode2 = datanodes[1];

            LOG.info("attempting to kill/suspend all the nodes not used for this test");
            Iterator<DNClient> iter = dfsCluster.getDNClients().iterator();
            int i = 0;
            while (iter.hasNext()) {
                try {
                    DNClient dn = iter.next();
                    // kill doesn't work with secure-HDFS, so using our stopDataNode() method
                    stopDatanode( dn );
                    i++;
                } catch (Exception e) {
                    LOG.info("error shutting down node " + i + ": " + e);
                }
            }

            LOG.info("attempting to kill both test nodes");
            // TODO add check to make sure there is enough capacity on these nodes to run test
            stopDatanode(datanode1);
            stopDatanode(datanode2);

            LOG.info("starting up datanode ["+
            datanode1.getHostName()+
            "] and loading it with data");
            startDatanode(datanode1);

            // mkdir balancer-temp
            balancerTempDir = makeTempDir();
            // TODO write 2 blocks to file system
            LOG.info("generating filesystem load");
            // TODO spec blocks to generate by blockCount, blockSize, # of writers
            generateFileSystemLoad(2);  // generate 2 blocks of test data

            LOG.info("measure space used on 1st node");
            long usedSpace0 = getDatanodeUsedSpace(datanode1);
            LOG.info("datanode " + datanode1.getHostName()
                    + " contains " + usedSpace0 + " bytes");

            LOG.info("bring up a 2nd node and run balancer on DFS");
            startDatanode(datanode2);
            runBalancer();

            //JMXListenerBean lsnr2 = JMXListenerBean.listenForDataNodeInfo(datanode2);

            LOG.info("measure blocks and files on both nodes, assert these "
                    + "counts are identical pre- and post-balancer run");
            long usedSpace1 = getDatanodeUsedSpace(datanode1);
            long usedSpace2 = getDatanodeUsedSpace(datanode2);
            long observedValue = usedSpace1 + usedSpace2;
            long expectedValue = usedSpace0;
            int errorTolerance = 10;
            double toleranceValue = expectedValue * (errorTolerance/100.0);
            String assertMsg =
                    String.format(
                    "The observed used space [%d] exceeds the expected "+
                    "used space [%d] by more than %d%% tolerance [%.2f]",
                    observedValue,  expectedValue,
                    errorTolerance, toleranceValue );
            Assert.assertTrue(
                    assertMsg,
                    withinTolerance(expectedValue, observedValue, errorTolerance) );
            LOG.info( String.format(
                    "The observed used space [%d] approximates expected "+
                    "used space [%d] within %d%% tolerance [%.2f]",
                     observedValue,  expectedValue,
                     errorTolerance, toleranceValue) );
        } catch (Throwable t) {
            LOG.info("method testBalancer failed", t);
        } finally {
            // finally block to run cleanup
            LOG.info("clean off test data from DFS [rmr ~/balancer-temp]");
            try {
                deleteTempDir(balancerTempDir);
            } catch (Exception e) {
                LOG.warn("problem cleaning up temp dir", e);
            }

            // restart killed nodes
            Iterator<DNClient> iter = dfsCluster.getDNClients().iterator();

            while (iter.hasNext()) {
                DNClient dn = iter.next();
                startDatanode( dn );
            }
        }
    }

    /** Kill all datanodes but 2, return a list of the reserved datanodes */
    private DNClient[] getReserveDatanodes() {
        List<DNClient> testDNs = new LinkedList<DNClient>();
        List<DNClient> dieDNs = new LinkedList<DNClient>();
        LOG.info("getting collection of live data nodes");
        NNClient namenode = dfsCluster.getNNClient();
        List<DNClient> dnList = dfsCluster.getDNClients();
        int dnCount = dnList.size();
        if (dnList.size() < 2) {
            // TODO throw a non-RuntimeException here instead
            String msg = String.format(
                    "not enough datanodes available to run test,"
                    + " need 2 datanodes but have only %d available",
                    dnCount);
            throw new RuntimeException(msg);
        }
        LOG.info("selecting 2 nodes for test");
        dieDNs = new LinkedList<DNClient>(dnList);
        testDNs = new LinkedList<DNClient>();

        final int LEN = dnCount - 1;
        int i = getRandom(LEN);
        DNClient testDN = dieDNs.get(i);
        testDNs.add(testDN);
        dieDNs.remove(testDN);
        int j = i;
        do {
            i = getRandom(LEN);
        } while (i != j);
        testDN = dieDNs.get(i);
        testDNs.add(testDN);
        dieDNs.remove(testDN);

        LOG.info("nodes reserved for test");
        printDatanodeList(testDNs);

        LOG.info("nodes not used in test");
        printDatanodeList(dieDNs);

        DNClient[] arr = new DNClient[]{};
        return (DNClient[]) testDNs.toArray(arr);
    }

    /**
     * Return a random number between 0 and N inclusive.
     *
     * @param int n
     * @param n  max number to return
     * @return random integer between 0 and N
     */
    private int getRandom(int n) {
        return (int) (n * Math.random());
    }

    /**
     * Calculate if the error in expected and observed values is within tolerance
     *
     * @param expectedValue  expected value of experiment
     * @param observedValue  observed value of experiment
     * @param tolerance      per cent tolerance for error, represented as a int
     */
    private boolean withinTolerance(long expectedValue,
                                    long observedValue,
                                    int tolerance) {
        double diff = 1.0 * Math.abs(observedValue - expectedValue);
        double thrs = expectedValue * (tolerance/100);
        return diff > thrs;
    }

    /**
     * Make a working directory for storing temporary files
     *
     * @throws IOException
     */
    private Path makeTempDir() throws IOException {
        Path temp = new Path(BALANCER_TEMP_DIR);
        FileSystem srcFs = temp.getFileSystem(hadoopConf);
        FileStatus fstatus = null;
        try {
            fstatus = srcFs.getFileStatus(temp);
            if (fstatus.isDir()) {
                LOG.warn(BALANCER_TEMP_DIR + ": File exists");
            } else {
                LOG.warn(BALANCER_TEMP_DIR + " exists but is not a directory");
            }
            deleteTempDir(temp);
        } catch (FileNotFoundException fileNotFoundExc) {
        } finally {
            if (!srcFs.mkdirs(temp)) {
                throw new IOException("failed to create " + BALANCER_TEMP_DIR);
            }
        }
        return temp;
    }

    /**
     * Remove the working directory used to store temporary files
     *
     * @param temp
     * @throws IOException
     */
    private void deleteTempDir(Path temp) throws IOException {
        FileSystem srcFs = temp.getFileSystem(hadoopConf);
        LOG.info("attempting to delete path " + temp + "; this path exists? -> " + srcFs.exists(temp));
        srcFs.delete(temp, true);
    }

    private void printDatanodeList(List<DNClient> lis) {
        for (DNClient datanode : lis) {
            LOG.info("\t" + datanode.getHostName());
        }
    }

    private final static String CMD_STOP_DN = "sudo yinst stop hadoop_datanode_admin";
    private void stopDatanode(DNClient dn) {
        String dnHost = dn.getHostName();
        runAndWatch(dnHost, CMD_STOP_DN);
    }
    private final static String CMD_START_DN = "sudo yinst start hadoop_datanode_admin";
    private void startDatanode(DNClient dn) {
        String dnHost = dn.getHostName();
        runAndWatch(dnHost, CMD_START_DN);
    }

    /* using "old" default block size of 64M */
    private static final int DFS_BLOCK_SIZE = 67108864;

    private void generateFileSystemLoad(int numBlocks) {
        String destfile = "hdfs:///user/hadoopqa/" + BALANCER_TEMP_DIR + "/LOADGEN.DAT";
        SecureRandom randgen = new SecureRandom();
        ByteArrayOutputStream dat = null;
        ByteArrayInputStream in = null;
        final int CHUNK = 4096;
        try {
            for (int i = 0; i < numBlocks; i++) {
                FileSystem fs = FileSystem.get(URI.create(destfile), hadoopConf);
                OutputStream out = fs.create(new Path(destfile), new ProgressReporter());
                dat = new ByteArrayOutputStream(DFS_BLOCK_SIZE);
                for (int z = 0; z < DFS_BLOCK_SIZE; z += CHUNK) {
                    byte[] bytes = new byte[CHUNK];
                    randgen.nextBytes(bytes);
                    dat.write(bytes, 0, CHUNK);
                }

                in = new ByteArrayInputStream(dat.toByteArray());
                IOUtils.copyBytes(in, out, CHUNK, true);
                LOG.info("wrote block " + (i + 1) + " of " + numBlocks);
            }
        } catch (IOException ioExc) {
            LOG.warn("f/s loadgen failed!", ioExc);
        } finally {
            try {
                dat.close();
            } catch (Exception e) {
            }
            try {
                in.close();
            } catch (Exception e) {
            }
        }
    }
    // TODO this should be taken from the environment
    public final static String HADOOP_HOME = "/grid/0/gs/gridre/yroot.biga/share/hadoop-current";
    public final static String CMD_SSH = "/usr/bin/ssh";
    public final static String CMD_KINIT = "/usr/kerberos/bin/kinit";
    public final static String CMD_HADOOP = HADOOP_HOME + "/bin/hadoop";
    public final static String OPT_BALANCER = "balancer";
    public final static String KERB_KEYTAB = "/homes/hadoopqa/hadoopqa.dev.headless.keytab";
    public final static String KERB_PRINCIPAL = "hadoopqa@DEV.YGRID.YAHOO.COM";

    private void runBalancer() throws IOException {
        String balancerCommand = String.format("\"%s -k -t %s %s; %s %s",
                CMD_KINIT,
                KERB_KEYTAB,
                KERB_PRINCIPAL,
                CMD_HADOOP,
                OPT_BALANCER);
        String nnHost = dfsCluster.getNNClient().getHostName();
        runAndWatch(nnHost, balancerCommand);
    }

    private void runAndWatch(String remoteHost, String remoteCommand) {
        try {
            Process proc = new ProcessBuilder(CMD_SSH, remoteHost, remoteCommand).start();
            watchProcStream(proc.getInputStream(), System.out);
            watchProcStream(proc.getErrorStream(), System.err);
            int exitVal = proc.waitFor();
        } catch(InterruptedException intExc) {
            LOG.warn("got thread interrupt error", intExc);
        } catch(IOException ioExc) {
            LOG.warn("got i/o error", ioExc);
        }
    }

    private void watchProcStream(InputStream in, PrintStream out) {
        new Thread(new StreamWatcher(in, out)).start();
    }
    private static final String DATANODE_VOLUME_INFO = "VolumeInfo";
    private static final String ATTRNAME_USED_SPACE="usedSpace";
    private long getDatanodeUsedSpace(DNClient datanode) throws IOException {
        Object volInfo = datanode.getDaemonAttribute(DATANODE_VOLUME_INFO);
        Assert.assertNotNull("Attribute %s should be non-null", volInfo);
        String strVolInfo = volInfo.toString();
        LOG.debug( String.format("Value of %s: %s",
                   DATANODE_VOLUME_INFO,
                   strVolInfo) );
        Map volInfoMap = (Map) JSON.parse(strVolInfo);
        long totalUsedSpace = 0L;
        for(Object key: volInfoMap.keySet()) {
            Map attrMap = (Map) volInfoMap.get(key);
            long usedSpace = (Long) attrMap.get(ATTRNAME_USED_SPACE);
            totalUsedSpace += usedSpace;
        }
        return totalUsedSpace;
    }

    /** simple utility to watch streams from an exec'ed process */
    static class StreamWatcher implements Runnable {

        private BufferedReader reader;
        private PrintStream printer;

        StreamWatcher(InputStream in, PrintStream out) {
            reader = getReader(in);
            printer = out;
        }

        private static BufferedReader getReader(InputStream in) {
            return new BufferedReader(new InputStreamReader(in));
        }

        public void run() {
            try {
                if (reader.ready()) {
                    printer.println(reader.readLine());
                }
            } catch (IOException ioExc) {
            }
        }
    }

    /** simple utility to report progress in generating data */
    static class ProgressReporter implements Progressable {

        StringBuffer buf = null;

        public void progress() {
            if (buf == null) {
                buf = new StringBuffer();
            }
            buf.append(".");
            if (buf.length() == 10000) {
                LOG.info("..........");
                buf = null;
            }
        }
    }

    /**
     * Balancer_01
     * Start balancer and check if the cluster is balanced after the run.
     * Cluster should end up in balanced state.
     */
    @Test
    public void testBalancerSimple() throws IOException {
        // run balancer on "normal"cluster cluster
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_02
     * Test a cluster with even distribution, then a new empty node is
     * added to the cluster.
     */
    @Test
    public void testBalancerEvenDistributionWithNewNodeAdded() throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_03
     * Bring up a 1-node DFS cluster. Set files replication factor to be 1
     * and fill up the node to 30% full. Then add an empty datanode.
     */
     @Test
     public void testBalancerSingleNodeClusterWithNewNodeAdded() throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Balancer_04
     * The same as _03 except that the empty new data node is on a
     * different rack.
     */
     @Test
     public void testBalancerSingleNodeClusterWithNewNodeAddedFromDifferentRack()
             throws IOException {
         throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Balancer_05
     * The same as _03 except that the empty new data node is half the
     * capacity as the old one.
     */
     @Test
     public void testBalancerSingleNodeClusterWithHalfCapacityNewNode() {
         throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Balancer_06
     * Bring up a 2-node cluster and fill one node to be 60% and the
     * other to be 10% full. All nodes are on different racks.
     */
     @Test
    public void testBalancerTwoNodeMultiRackCluster() {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_07
     * Bring up a dfs cluster with nodes A and B. Set file replication
     * factor to be 2 and fill up the cluster to 30% full. Then add an
     * empty data node C. All three nodes are on the same rack.
     */
     @Test
     public void testBalancerTwoNodeSingleRackClusterWuthNewNodeAdded()
             throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Balancer_08
     * The same as _07 except that A, B and C are on different racks.
     */
     @Test
     public void testBalancerTwoNodeMultiRackClusterWithNewNodeAdded()
             throws IOException {
         throw new UnsupportedOperationException("not implemented yet!");
     }

     /**
     * Balancer_09
     * The same as _07 except that interrupt balancing.
     */
     @Test
     public void testBalancerTwoNodeSingleRackClusterInterruptingRebalance()
             throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_10
     * Restart rebalancing until it is done.
     */
    @Test
    public void testBalancerRestartInterruptedBalancerUntilDone()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_11
     * The same as _07 except that the namenode is shutdown while rebalancing.
     */
    @Test
    public void testBalancerTwoNodeSingleRackShutdownNameNodeDuringRebalance()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_12
     * The same as _05 except that FS writes occur during rebalancing.
     */
    @Test
    public void
    testBalancerSingleNodeClusterWithHalfCapacityNewNodeRebalanceWithConcurrentFSWrites()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_13
     * The same as _05 except that FS deletes occur during rebalancing.
     */
    @Test
    public void testBalancerSingleNodeClusterWithHalfCapacityNewNodeRebalanceWithConcurrentFSDeletes()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_14
     * The same as _05 except that FS deletes AND writes occur during
     * rebalancing.
     */
    @Test
    public void testBalancerSingleNodeClusterWithHalfCapacityNewNodeRebalanceWithConcurrentFSDeletesAndWrites()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_15
     * Scalability test: Populate a 750-node cluster, then
     *    1. Run rebalancing after 3 nodes are added
     *    2. Run rebalancing after 2 racks of nodes (60 nodes) are added
     *    3. Run rebalancing after 2 racks of nodes are added and concurrently
     *       executing file writing and deleting at the same time
     */
    @Test
    public void testBalancerScalability() throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_16
     * Start balancer with a negative threshold value.
     */
    @Test
    public void testBalancerConfiguredWithThresholdValueNegative()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_17
     * Start balancer with out-of-range threshold value
     *  (e.g. -123, 0, -324, 100000, -12222222, 1000000000, -10000, 345, 989)
     */
    @Test
    public void testBalancerConfiguredWithThresholdValueOutOfRange()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_18
     * Start balancer with alpha-numeric threshold value
     *  (e.g., 103dsf, asd234, asfd, ASD, #$asd, 2345&, $35, %34)
     */
    @Test
    public void testBalancerConfiguredWithThresholdValueAlphanumeric()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_19
     * Start 2 instances of balancer on the same gateway
     */
    @Test
    public void testBalancerRunTwoConcurrentInstancesOnSingleGateway()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_20
     * Start 2 instances of balancer on two different gateways
     */
    @Test
    public void testBalancerRunTwoConcurrentInstancesOnDistinctGateways()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_21
     * Start balancer when the cluster is already balanced
     */
    @Test
    public void testBalancerOnBalancedCluster() throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_22
     * Running the balancer with half the data nodes not running
     */
     @Test
     public void testBalancerWithOnlyHalfOfDataNodesRunning()
             throws IOException {
         throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Balancer_23
     * Running the balancer and simultaneously simulating load on the
     * cluster with half the data nodes not running.
     */
     @Test
     public void testBalancerOnBusyClusterWithOnlyHalfOfDatanodesRunning()
             throws IOException {
         throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Protocol Test Prelude
     *
     * First set up 3 node cluster with nodes NA, NB and NC, which are on
     * different racks. Then create a file with one block B with a replication
     * factor 3. Finally add a new node ND to the cluster on the same rack as NC.
     */

    /**
     * ProtocolTest_01
     * Copy block B from ND to NA with del hint NC
     */
    @Test
    public void
    testBlockReplacementProtocolFailWhenCopyBlockSourceDoesNotHaveBlockToCopy()
            throws IOException {
         throw new UnsupportedOperationException("not implemented yet!");
    }

    /*
     * ProtocolTest_02
     * Copy block B from NA to NB with del hint NB
     */
    @Test
    public void
    testBlockReplacementProtocolFailWhenCopyBlockDestinationContainsBlockCopy()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * ProtocolTest_03
     * Copy block B from NA to ND with del hint NB
     */
    @Test
    public void testBlockReplacementProtocolCopyBlock() throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * ProtocolTest_04
     * Copy block B from NB to NC with del hint NA
     */
    @Test
    public void testBlockReplacementProtocolWithInvalidHint()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * ThrottleTest_01
     * Create a throttler with 1MB/s bandwidth. Send 6MB data, and throttle
     * at 0.5MB, 0.75MB, and in the end [1MB/s?].
     */

    /**
     * NamenodeProtocolTest_01
     * Get blocks from datanode 0 with a size of 2 blocks.
     */
    @Test
    public void testNamenodeProtocolGetBlocksCheckThroughput()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * NamenodeProtocolTest_02
     * Get blocks from datanode 0 with a size of 1 block.
     */
    @Test
    public void testNamenodeProtocolGetSingleBlock()
            throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * NamenodeProtocolTest_03
     * Get blocks from datanode 0 with a size of 0.
     */
    @Test
    public void testNamenodeProtocolGetZeroBlocks() throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");
    }
    /**
     * NamenodeProtocolTest_04
     * Get blocks from datanode 0 with a size of -1.
     */
    @Test
    public void testNamenodeProtocolGetMinusOneBlocks() throws Exception {

    }

    /**
     * NamenodeProtocolTest_05
     * Get blocks from a non-existent datanode.
     */
    @Test
    public void testNamenodeProtocolGetBlocksFromNonexistentDatanode()
            throws IOException {

    }
}

