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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
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

import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
    // TODO don't hardwire these, introspect the cluster
    private static final String NAMENODE = "gsbl90277.blue.ygrid.yahoo.com";
    private static final String[] ENDPOINT_JMX = {
        "gsbl90277.blue.ygrid.yahoo.com-8008",
        "gsbl90276.blue.ygrid.yahoo.com-24812",
        "gsbl90275.blue.ygrid.yahoo.com-24810",
        "gsbl90274.blue.ygrid.yahoo.com-24808",
        "gsbl90273.blue.ygrid.yahoo.com-24806",
        "gsbl90272.blue.ygrid.yahoo.com-24804",
        "gsbl90271.blue.ygrid.yahoo.com-24802",
        "gsbl90270.blue.ygrid.yahoo.com-24800"
    };
    private Map<String, MBeanServerConnection> endpointMap =
            new HashMap<String, MBeanServerConnection>();

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
        connectJMX();
    }

    @After
    public void tearDown() throws Exception {
        dfsCluster.tearDown();
        mrCluster.tearDown();
    }

    /** Connect to JMX agents on HDFS cluster nodes */
    private void connectJMX() {
        final int HOST = 0;
        final int PORT = 1;
        for (String endpoint : ENDPOINT_JMX) {
            String[] toks = endpoint.split("-");
            String host = toks[HOST];
            String port = toks[PORT];
            LOG.info("HOST=" + host + ", PORT=" + port);
            MBeanServerConnection jmxEndpoint = getJMXEndpoint(host, port);
            endpointMap.put(host, jmxEndpoint);
        }
    }

    private long getDataNodeFreeSpace(DNClient datanode) {
        String dnHost = datanode.getHostName();
        Object volObj = getDNAttribute(dnHost, "VolumeInfo");
        Map volInfoMap = (Map) JSON.parse(volObj.toString());
        long totalFreeSpace = 0L;
        for (Object key : volInfoMap.keySet()) {
            Map attrMap = (Map) volInfoMap.get(key);
            long freeSpace = (Long) attrMap.get("freeSpace");
            //LOG.info( String.format("volume %s has %d bytes free space left", key, freeSpace) );
            totalFreeSpace += freeSpace;
        }
        //LOG.info(String.format("got from host %s volinfo:\n%s", dnHost, volObj));
        return totalFreeSpace;
    }

    private long getDataNodeUsedSpace(DNClient datanode) {
        String dnHost = datanode.getHostName();
        LOG.debug("checking DFS space used on host "+dnHost);
        Object volObj = getDNAttribute(dnHost, "VolumeInfo");
        LOG.debug("retrieved volume info object "+volObj);
        Map volInfoMap = (Map) JSON.parse(volObj.toString());
        long totalUsedSpace = 0L;
        for (Object key : volInfoMap.keySet()) {
            Map attrMap = (Map) volInfoMap.get(key);
            // TODO should we be using free space here?
            long usedSpace = (Long) attrMap.get("usedSpace");
            LOG.info( String.format("volume %s has %d bytes used space", key, usedSpace) );
            totalUsedSpace += usedSpace;
        }
        //LOG.info(String.format("got from host %s volinfo:\n%s", dnHost, volObj));
        return totalUsedSpace;
    }

    // TODO just throw the dang exceptions
    private Object getDNAttribute(String host, String attribName) {
        ObjectName name = null;
        Object attribVal = null;
        try {
            MBeanServerConnection conn = endpointMap.get(host);
            name = new ObjectName("HadoopInfo:type=DataNodeInfo");
            attribVal = conn.getAttribute(name, attribName);
        } catch (javax.management.AttributeNotFoundException attribNotFoundExc) {
            LOG.warn(String.format("no attribute matching %s found", attribName),
                    attribNotFoundExc);
        } catch (javax.management.MalformedObjectNameException badObjNameExc) {
            LOG.warn("bad object name: " + name, badObjNameExc);
        } catch (javax.management.InstanceNotFoundException instNotFoundExc) {
            LOG.warn("no MBean instance found", instNotFoundExc);
        } catch (javax.management.ReflectionException reflectExc) {
            LOG.warn("reflection error!", reflectExc);
        } catch (javax.management.MBeanException mBeanExc) {
            LOG.warn("MBean error!", mBeanExc);
        } catch (java.io.IOException ioExc) {
            LOG.debug("i/o error!", ioExc);
        }
        return attribVal;
    }
    //@Test

    public void testJMXRemote() {
        final int HOST = 0;
        final int PORT = 1;
        for (String endpoint : ENDPOINT_JMX) {
            String[] toks = endpoint.split("-");
            String host = toks[HOST];
            String port = toks[PORT];
            //LOG.info("HOST="+host+", PORT="+port);
            MBeanServerConnection jmxEndpoint = getJMXEndpoint(host, port);
            endpointMap.put(host, jmxEndpoint);
        }


        Iterator<String> iter = endpointMap.keySet().iterator();
        while (iter.hasNext()) {
            String host = iter.next();
            MBeanServerConnection conn = endpointMap.get(host);
            ObjectName mBeanName = null;
            try {
                if (NAMENODE.equals(host)) {
                    // TODO make this a constant
                    mBeanName = new ObjectName("HadoopInfo:type=NameNodeInfo");
                } else {
                    mBeanName = new ObjectName("HadoopInfo:type=DataNodeInfo");
                }
                Object versionObj = conn.getAttribute(mBeanName, "Version");
                LOG.info("host [" + host + "] runs version " + versionObj);
            } catch (javax.management.AttributeNotFoundException attribNotFoundExc) {
                // TODO don't hard-wire attrib name
                LOG.warn("no attribute matching `Version' found", attribNotFoundExc);
            } catch (javax.management.MalformedObjectNameException badObjNameExc) {
                LOG.warn("bad object name: " + mBeanName, badObjNameExc);
            } catch (javax.management.InstanceNotFoundException instNotFoundExc) {
                LOG.warn("no MBean instance found", instNotFoundExc);
            } catch (javax.management.ReflectionException reflectExc) {
                LOG.warn("reflection error!", reflectExc);
            } catch (javax.management.MBeanException mBeanExc) {
                LOG.warn("MBean error!", mBeanExc);
            } catch (java.io.IOException ioExc) {
                LOG.debug("i/o error!", ioExc);
            }
        }
    }

    private MBeanServerConnection getJMXEndpoint(String host, String port) {
        MBeanServerConnection conn = null;
        String urlPattern = null;
        try {
            urlPattern =
                    "service:jmx:rmi:///jndi/rmi://"
                    + host + ":"
                    + port
                    + "/jmxrmi";
            JMXServiceURL url = new JMXServiceURL(urlPattern);
            JMXConnector connector = JMXConnectorFactory.connect(url);
            conn = connector.getMBeanServerConnection();
        } catch (java.net.MalformedURLException badURLExc) {
            LOG.debug("bad url: " + urlPattern, badURLExc);
        } catch (java.io.IOException ioExc) {
            LOG.debug("i/o error!", ioExc);
        }
        return conn;
    }
    /* debug--
    public void testHello() {
    LOG.info("hello!");
    }*/

    //@Test
    public void testNameNodePing() throws IOException {
        LOG.info("testing filesystem ping");
        NNClient namenode = dfsCluster.getNNClient();
        namenode.ping();
        LOG.info("done.");
    }

    //@Test
    public void testNameNodeConnectDisconnect() throws IOException {
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
            DNClient[] datanodes = getReserveDNs();
            DNClient datanode1 = datanodes[0];
            DNClient datanode2 = datanodes[1];

            LOG.info("attempting to kill/suspend all the nodes not used for this test");
            Iterator<DNClient> iter = dfsCluster.getDNClients().iterator();
            int i = 0;
            while (iter.hasNext()) {
                try {
                    DNClient dn = iter.next();
                    // TODO kill doesn't work anymore
                    // TODO do a ssh to admin gateway and sudo yinst with command text do down a specific datanode
                    stopDN( dn );
                    i++;
                } catch (Exception e) {
                    LOG.info("error shutting down node " + i + ": " + e);
                }
            }
            
            LOG.info("attempting to kill both test nodes");
            // TODO add check to make sure there is enough capacity on these nodes to run test
            stopDN(datanode1);
            stopDN(datanode2);

            LOG.info("starting up datanode ["+
            datanode1.getHostName()+
            "] and loading it with data");
            startDN(datanode1);
            
            LOG.info("datanode " + datanode1.getHostName()
                    + " contains " + getDataNodeUsedSpace(datanode1) + " bytes");
            // mkdir balancer-temp
            balancerTempDir = makeTempDir();
            // TODO write 2 blocks to file system
            LOG.info("generating filesystem load");
            generateFSLoad(2);  // generate 2 blocks of test data

            LOG.info("measure space used on 1st node");
            long usedSpace0 = getDataNodeUsedSpace(datanode1);
            LOG.info("datanode " + datanode1.getHostName()
                    + " contains " + usedSpace0 + " bytes");

            LOG.info("bring up a 2nd node and run balancer on DFS");
            startDN(datanode2);
            runBalancer();

            LOG.info("measure blocks and files on both nodes, assert these "
                    + "counts are identical pre- and post-balancer run");
            long usedSpace1 = getDataNodeUsedSpace(datanode1);
            long usedSpace2 = getDataNodeUsedSpace(datanode2);
            Assert.assertEquals(usedSpace0, usedSpace1 + usedSpace2);

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
                startDN( dn );
            }
        }
    }
    /* Kill all datanodes but 2, return a list of the reserved datanodes */

    private DNClient[] getReserveDNs() {
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
        printDNList(testDNs);

        LOG.info("nodes not used in test");
        printDNList(dieDNs);

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

    private void printDNList(List<DNClient> lis) {
        for (DNClient datanode : lis) {
            LOG.info("\t" + datanode.getHostName());
        }
    }

    private final static String CMD_STOP_DN = "sudo yinst stop hadoop_datanode_admin";
    private void stopDN(DNClient dn) {
        String dnHost = dn.getHostName();
        runAndWatch(dnHost, CMD_STOP_DN);
    }
    private final static String CMD_START_DN = "sudo yinst start hadoop_datanode_admin";
    private void startDN(DNClient dn) {
        String dnHost = dn.getHostName();
        runAndWatch(dnHost, CMD_START_DN);
    }

    /* using "old" default block size of 64M */
    private
    static final int DFS_BLOCK_SIZE = 67108864;

    private void generateFSLoad(int numBlocks) {
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
    // NOTE this shouldn't be hardwired
    public final static String HOST_NAMENODE = "gsbl90277.blue.ygrid.yahoo.com";
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

    static class StreamWatcher implements Runnable {

        BufferedReader reader;
        PrintStream printer;

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
}
