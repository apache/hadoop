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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.test.system.HDFSCluster;
import org.apache.hadoop.hdfs.test.system.NNClient;
import org.apache.hadoop.hdfs.test.system.DNClient;


import org.apache.hadoop.io.IOUtils;
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

    public TestBalancer() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        hadoopConf = new Configuration();
        dfsCluster = HDFSCluster.createCluster(hadoopConf);
        dfsCluster.setUp();
    }

    @After
    public void tearDown() throws Exception {
        dfsCluster.tearDown();
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
        Path balancerTempDir = null;
        try {
            List<DNClient> testnodes = reserveDatanodesForTest(2);
            DNClient testnode1 = testnodes.get(0);
            DNClient testnode2 = testnodes.get(1);
            shutdownNonTestNodes(testnodes);

            LOG.info("attempting to kill both test nodes");
            stopDatanode(testnode1);
            stopDatanode(testnode2);

            LOG.info("starting up datanode ["+
            testnode1.getHostName()+
            "] and loading it with data");
            startDatanode(testnode1);

            // mkdir balancer-temp
            balancerTempDir = makeTempDir();
            // write 2 blocks to file system
            LOG.info("generating filesystem load");
            // TODO spec blocks to generate by blockCount, blockSize, # of writers
            generateFileSystemLoad(2);  // generate 2 blocks of test data

            LOG.info("measure space used on 1st node");
            long usedSpace0 = getDatanodeUsedSpace(testnode1);
            LOG.info("datanode " + testnode1.getHostName()
                    + " contains " + usedSpace0 + " bytes");

            LOG.info("bring up a 2nd node and run balancer on DFS");
            startDatanode(testnode2);
            runBalancerAndVerify(testnodes);
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

    private void shutdownNonTestNodes(List<DNClient> testnodes) {
        Set killSet = new HashSet(getAllDatanodes());
        killSet.removeAll(testnodes);
        LOG.info("attempting to kill/suspend all the nodes not used for this test");
        Iterator<DNClient> iter = killSet.iterator();
        DNClient dn = null;
        while (iter.hasNext()) {
            dn = iter.next();
            // kill may not work with some secure-HDFS configs,
            // so using our stopDataNode() method
            stopDatanode(dn);
        }
    }

    /** 
     * Kill all datanodes but leave reservationCount nodes alive,
     * return a list of the reserved datanodes
     */
    private List<DNClient> reserveDatanodesForTest(int reservationCount) {
        List<DNClient> testDNs = new LinkedList<DNClient>();
        List<DNClient> dieDNs = new LinkedList<DNClient>();
        LOG.info("getting collection of live data nodes");
        List<DNClient> dnList = getAllDatanodes();
        int dnCount = dnList.size();
        //  check to make sure there is enough capacity on these nodes to run test
        Assert.assertTrue(
                String.format(
                    "not enough datanodes available to run test,"
                    + " need %d datanodes but have only %d available",
                    reservationCount, dnCount),
                ( dnCount >= reservationCount ));
        LOG.info("selecting "+reservationCount+" nodes for test");
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

        return testDNs;
    }

    private List<DNClient> getAllDatanodes() {
        return dfsCluster.getDNClients();
    }

    private final static DNClient[] DATANODE_ARRAY = {};
    private DNClient[] toDatanodeArray(List<DNClient> datanodeList) {
        return (DNClient[]) datanodeList.toArray(DATANODE_ARRAY);
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

    //  emulate tolerance calculation in balancer code
    public final static int DEFAULT_TOLERANCE = 10; // 10%
    protected boolean isClusterBalanced(DNClient[] datanodes) throws IOException {
        return isClusterBalanced(datanodes, DEFAULT_TOLERANCE);
    }
    protected boolean isClusterBalanced(DNClient[] datanodes, int tolerance)
            throws IOException {

        Assert.assertFalse("empty datanode array specified",
                ArrayUtils.isEmpty(datanodes));
        boolean result = true;
        double[] utilizationByNode = new double[ datanodes.length ];
        double totalUsedSpace = 0L;
        double totalCapacity = 0L;
        Map datanodeVolumeMap = new HashMap();
        // accumulate space stored on each node
        for(int i=0; i<datanodes.length; i++) {
            DNClient datanode = datanodes[i];
            Map volumeInfoMap = getDatanodeVolumeAttributes(datanode);
            long usedSpace = (Long)volumeInfoMap.get(ATTRNAME_USED_SPACE);
            long capacity  = (Long)volumeInfoMap.get(ATTRNAME_CAPACITY  );
            utilizationByNode[i] = ( ((double)usedSpace)/capacity ) * 100;
            totalUsedSpace += usedSpace;
            totalCapacity  += capacity;
        }
        // here we are reusing previously fetched volume-info, for speed
        // an alternative is to get fresh values from the cluster here instead
        double avgUtilization = ( totalUsedSpace/totalCapacity ) * 100;
        for(int i=0; i<datanodes.length; i++) {
            double varUtilization = Math.abs(avgUtilization - utilizationByNode[i]);
            if(varUtilization > tolerance) {
                result = false;
                break;
            }
        }

        return result;
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
    private static final short DEFAULT_REPLICATION = 3;
    private void generateFileSystemLoad(long numBlocks) {
        generateFileSystemLoad(numBlocks, DEFAULT_REPLICATION);
    }
    private void generateFileSystemLoad(long numBlocks, short replication) {
        String destfile = "hdfs:///user/hadoopqa/";// + BALANCER_TEMP_DIR + "/LOADGEN.DAT";
        SecureRandom randgen = new SecureRandom();
        ByteArrayOutputStream dat = null;
        ByteArrayInputStream in = null;
        final int CHUNK = 4096;
        final Configuration testConf = new Configuration(hadoopConf);
        try {
            testConf.setInt("dfs.replication", replication);
            for (int i = 0; i < numBlocks; i++) {
                FileSystem fs = FileSystem.get(
                        URI.create(destfile), testConf);
                OutputStream out = fs.create(
                        new Path(destfile),
                        replication,
                        new ProgressReporter());
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

    public final static int DEFAULT_THRESHOLD = 10;
    private int runBalancer() throws IOException {
        return runBalancer(DEFAULT_THRESHOLD);
    }

    private int runBalancer(int threshold) throws IOException {
        return runBalancer(""+threshold);
    }
    /*
     * TODO change the heap size balancer uses so it can run on gateways
     * i.e., 14G heap is too big for gateways
     */
    private int runBalancer(String threshold) 
            throws IOException {

        String balancerCommand = String.format("\"%s -k -t %s %s; %s %s -threshold %s",
                CMD_KINIT,
                KERB_KEYTAB,
                KERB_PRINCIPAL,
                CMD_HADOOP,
                OPT_BALANCER,
                threshold);
        String nnHost = dfsCluster.getNNClient().getHostName();
        return runAndWatch(nnHost, balancerCommand);
    }
    private void runBalancerAndVerify(List<DNClient> testnodes) 
            throws IOException {
        runBalancerAndVerify(testnodes, DEFAULT_THRESHOLD);
    }
    private void runBalancerAndVerify(List<DNClient> testnodes, int threshold)
            throws IOException {
        runBalancerAndVerify(testnodes, ""+DEFAULT_THRESHOLD);
    }
      private void runBalancerAndVerify(List<DNClient> testnodes, String threshold)
            throws IOException {
        int exitStatus = runBalancer(threshold);
        // assert balancer exits with status SUCCESSe
        Assert.assertTrue(
                String.format("balancer returned non-success exit code: %d",
                exitStatus),
                (exitStatus == SUCCESS));
        DNClient[] testnodeArr = toDatanodeArray(testnodes);
        Assert.assertTrue(
                "cluster is not balanced",
                isClusterBalanced(testnodeArr));
    }

    private int runAndWatch(String remoteHost, String remoteCommand) {
        int exitStatus = -1;
        try {
            Process proc = new ProcessBuilder(CMD_SSH, remoteHost, remoteCommand).start();
            watchProcStream(proc.getInputStream(), System.out);
            watchProcStream(proc.getErrorStream(), System.err);
            exitStatus = proc.waitFor();
        } catch(InterruptedException intExc) {
            LOG.warn("got thread interrupt error", intExc);
        } catch(IOException ioExc) {
            LOG.warn("got i/o error", ioExc);
        }
        return exitStatus;
    }

    private void watchProcStream(InputStream in, PrintStream out) {
        new Thread(new StreamWatcher(in, out)).start();
    }
    private static final String DATANODE_VOLUME_INFO = "VolumeInfo";
    private static final String ATTRNAME_USED_SPACE  = "usedSpace";
    private static final String ATTRNAME_FREE_SPACE  = "freeSpace";
    // pseudo attribute, JMX doesn't really provide this
    private static final String ATTRNAME_CAPACITY    = "capacity";
    // TODO maybe the static methods below belong in some utility class...
    private static long getDatanodeUsedSpace(DNClient datanode)
            throws IOException {
        return (Long)getDatanodeVolumeAttributes(datanode).get(ATTRNAME_USED_SPACE);
    }/*
    private static long getDatanodeFreeSpace(DNClient datanode)
            throws IOException {
        return (Long)getDatanodeVolumeAttributes(datanode).get(ATTRNAME_FREE_SPACE);
    }*/
    private static Map getDatanodeVolumeAttributes(DNClient datanode)
            throws IOException {
        Map result = new HashMap();
        long usedSpace = getVolumeAttribute(datanode, ATTRNAME_USED_SPACE);
        long freeSpace = getVolumeAttribute(datanode, ATTRNAME_FREE_SPACE);
        result.put(ATTRNAME_USED_SPACE, usedSpace);
        result.put(ATTRNAME_CAPACITY,   usedSpace+freeSpace);
        return result;
    }

    private static long getVolumeAttribute(DNClient datanode,
                                                 String attribName)
        throws IOException {

        Object volInfo = datanode.getDaemonAttribute(DATANODE_VOLUME_INFO);
        Assert
        .assertNotNull( String
                        .format( "Attribute \"%s\" should be non-null",
                                 DATANODE_VOLUME_INFO ),
                        volInfo );
        String strVolInfo = volInfo.toString();
        LOG.debug( String.format("Value of %s: %s",
                   DATANODE_VOLUME_INFO,
                   strVolInfo) );
        Map volInfoMap = (Map) JSON.parse(strVolInfo);
        long attrVal = 0L;
        for(Object key: volInfoMap.keySet()) {
            Map attrMap = (Map) volInfoMap.get(key);
            long val = (Long) attrMap.get(attribName);
            attrVal += val;
        }
        return attrVal;

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

    // A constant for SUCCESS exit code
     static final int SUCCESS = 1;

     /**
     * Balancer_01
     * Start balancer and check if the cluster is balanced after the run.
     * Cluster should end up in balanced state.
     */
    @Test
    public void testBalancerSimple() throws IOException {

        DNClient[] datanodes = toDatanodeArray( getAllDatanodes() );
        int exitStatus = runBalancer();
        // assert on successful exit code here
        Assert.assertTrue(
                String.format("balancer returned non-success exit code: %d",
                              exitStatus),
                (exitStatus == SUCCESS));
        Assert.assertTrue( "cluster is not balanced", isClusterBalanced(datanodes) );

    }

    /**
     * Balancer_02
     * Test a cluster with even distribution, then a new empty node is
     * added to the cluster. Here, even distribution effectively means the
     * cluster is in "balanced" state, as bytes consumed for block allocation
     * are evenly distributed throughout the cluster.
     */
    @Test
    public void testBalancerEvenDistributionWithNewNodeAdded() throws IOException {
        throw new UnsupportedOperationException("not implemented yet!");

        // get all nodes
        // need to get an external reserve of nodes we can boot up
        // to add to this cluster?
        // HOW?

        // IDEA try to steal some nodes from omega-M for now.....
        // hmmm also need a way to give an alternate "empty-node" config
        // to "hide" the data that may already exist on this node
    }

    /**
     * Balancer_03
     * Bring up a 1-node DFS cluster. Set files replication factor to be 1
     * and fill up the node to 30% full. Then add an empty datanode.
     */
     @Test
     public void testBalancerSingleNodeClusterWithNewNodeAdded() throws IOException {
        // empty datanode: mod config to point to non-default blocks dir.
        // limit capacity to available storage space
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
         // need rack awareness
         throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Balancer_05
     * The same as _03 except that the empty new data node is half the
     * capacity as the old one.
     */
     @Test
     public void testBalancerSingleNodeClusterWithHalfCapacityNewNode() {
         // how to limit node capacity?
         throw new UnsupportedOperationException("not implemented yet!");
     }

    /**
     * Balancer_06
     * Bring up a 2-node cluster and fill one node to be 60% and the
     * other to be 10% full. All nodes are on different racks.
     */
     @Test
    public void testBalancerTwoNodeMultiRackCluster() {
         // need rack awareness
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

        final short TEST_REPLICATION_FACTOR = 3;
        List<DNClient> testnodes = reserveDatanodesForTest(3);
        DNClient dnA = testnodes.get(0);
        DNClient dnB = testnodes.get(1);

        DNClient dnC = testnodes.get(2);
        stopDatanode(dnC);

        // change test: 30% full-er (ie, 30% over pre-test capacity),
        // use most heavily node as baseline
        long targetLoad = (long) ( 
                (1/DFS_BLOCK_SIZE) *
                0.30 *
            Math.max( getDatanodeUsedSpace(dnA), getDatanodeUsedSpace(dnB) ) );
        generateFileSystemLoad(targetLoad, TEST_REPLICATION_FACTOR);
        startDatanode(dnC);
        runBalancerAndVerify(testnodes);
     }

    /**
     * Balancer_08
     * The same as _07 except that A, B and C are on different racks.
     */
     @Test
     public void testBalancerTwoNodeMultiRackClusterWithNewNodeAdded()
             throws IOException {
         // need rack awareness
         throw new UnsupportedOperationException("not implemented yet!");
     }

     /**
     * Balancer_09
     * The same as _07 except that interrupt balancing.
     */
     @Test
     public void testBalancerTwoNodeSingleRackClusterInterruptingRebalance()
             throws IOException {
         // interrupt thread
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_10
     * Restart rebalancing until it is done.
     */
    @Test
    public void testBalancerRestartInterruptedBalancerUntilDone()
            throws IOException {
        // need kill-restart thread
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_11
     * The same as _07 except that the namenode is shutdown while rebalancing.
     */
    @Test
    public void testBalancerTwoNodeSingleRackShutdownNameNodeDuringRebalance()
            throws IOException {
        // need NN shutdown thread in addition
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
        // writer thread
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_13
     * The same as _05 except that FS deletes occur during rebalancing.
     */
    @Test
    public void testBalancerSingleNodeClusterWithHalfCapacityNewNodeRebalanceWithConcurrentFSDeletes()
            throws IOException {
        // eraser thread
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
        // writer & eraser threads
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
        /* work in progress->
         *
         *
        List<DNClient> dnList = getAllDatanodes();
        int dnCount = dnList.size();

        Assert.assertTrue(
                String.format(
                    "not enough datanodes available to run test,"
                    + " need 2 datanodes but have only %d available",
                    dnCount),
                ( dnCount == (875 - 2) ));

        List<DNClient> datanodes = reserveDatanodesForTest(750);
        shutdownNonTestNodes(datanodes);
        */
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_16
     * Start balancer with a negative threshold value.
     */
    @Test
    public void testBalancerConfiguredWithThresholdValueNegative()
            throws IOException {
        List<DNClient> testnodes = getAllDatanodes();
        final int TRIALS=5;
        for(int i=0; i<TRIALS; i++) {
            int negThreshold = (int)(-1 * 100 * Math.random());
            runBalancerAndVerify(testnodes, negThreshold);
        }
    }

    /**
     * Balancer_17
     * Start balancer with out-of-range threshold value
     *  (e.g. -123, 0, -324, 100000, -12222222, 1000000000, -10000, 345, 989)
     */
    @Test
    public void testBalancerConfiguredWithThresholdValueOutOfRange()
            throws IOException {
        List<DNClient> testnodes = getAllDatanodes();
        final int[] THRESHOLD_OUT_OF_RANGE_DATA = {
            -123, 0, -324, 100000, -12222222, 1000000000, -10000, 345, 989
        };
        for(int threshold: THRESHOLD_OUT_OF_RANGE_DATA) {
            runBalancerAndVerify(testnodes, threshold);
        }
    }

    /**
     * Balancer_18
     * Start balancer with alpha-numeric threshold value
     *  (e.g., 103dsf, asd234, asfd, ASD, #$asd, 2345&, $35, %34)
     */
    @Test
    public void testBalancerConfiguredWithThresholdValueAlphanumeric()
            throws IOException {
        List<DNClient> testnodes = getAllDatanodes();
        final String[] THRESHOLD_ALPHA_DATA = {
            "103dsf", "asd234", "asfd", "ASD", "#$asd", "2345&", "$35", "%34", 
            "0x64", "0xde", "0xad", "0xbe", "0xef"
        };
        for(String threshold: THRESHOLD_ALPHA_DATA) {
            runBalancerAndVerify(testnodes,threshold);
        }
    }

    /**
     * Balancer_19
     * Start 2 instances of balancer on the same gateway
     */
    @Test
    public void testBalancerRunTwoConcurrentInstancesOnSingleGateway()
            throws IOException {
        // do on gateway logic with small balancer heap
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_20
     * Start 2 instances of balancer on two different gateways
     */
    @Test
    public void testBalancerRunTwoConcurrentInstancesOnDistinctGateways()
            throws IOException {
            // do on gateway logic with small balancer heap
        throw new UnsupportedOperationException("not implemented yet!");
    }

    /**
     * Balancer_21
     * Start balancer when the cluster is already balanced
     */
    @Test
    public void testBalancerOnBalancedCluster() throws IOException {
        // run balancer twice
        testBalancerSimple();
        testBalancerSimple();
    }

    /**
     * Balancer_22
     * Running the balancer with half the data nodes not running
     */
     @Test
     public void testBalancerWithOnlyHalfOfDataNodesRunning()
            throws IOException {
        List<DNClient> datanodes = getAllDatanodes();
        int testnodeCount = (int)Math.floor(datanodes.size() * 0.5);
        List<DNClient> testnodes = reserveDatanodesForTest(testnodeCount);
        runBalancerAndVerify(testnodes);
    }

    /**
     * Balancer_23
     * Running the balancer and simultaneously simulating load on the
     * cluster with half the data nodes not running.
     */
     @Test
     public void testBalancerOnBusyClusterWithOnlyHalfOfDatanodesRunning()
             throws IOException {
         // load thread
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
        final short replication = 1;
        Path balancerTempDir = null;
        try {
        // reserve 2 nodes for test
        List<DNClient> testnodes = reserveDatanodesForTest(2);
        shutdownNonTestNodes(testnodes);

        DNClient testnode1 = testnodes.get(0);
        DNClient testnode2 = testnodes.get(1);

        // write some blocks with replication factor of 1
        balancerTempDir = makeTempDir();
        generateFileSystemLoad(20, replication);

        // get block locations from NN
        NNClient namenode = dfsCluster.getNNClient();
        // TODO extend namenode to get block locations
        //namenode.get

        // shutdown 1 node
        stopDatanode(testnode1);
        
        // attempt to retrieve blocks from the dead node
        // we should fail
        } finally {
            // cleanup
                        // finally block to run cleanup
            LOG.info("clean off test data from DFS [rmr ~/balancer-temp]");
            try {
                deleteTempDir(balancerTempDir);
            } catch (Exception e) {
                LOG.warn("problem cleaning up temp dir", e);
            }
        }
    }
}

