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
    public void testNameNodePing() throws IOException {
        LOG.info("testing filesystem ping");
        NNClient namenode = dfsCluster.getNNClient();
        namenode.ping();
        LOG.info("done.");
    }

    // Trivial @Test
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
            DNClient[] datanodes = getReserveDataNodes();
            DNClient datanode1 = datanodes[0];
            DNClient datanode2 = datanodes[1];

            LOG.info("attempting to kill/suspend all the nodes not used for this test");
            Iterator<DNClient> iter = dfsCluster.getDNClients().iterator();
            int i = 0;
            while (iter.hasNext()) {
                try {
                    DNClient dn = iter.next();
                    // kill doesn't work with secure-HDFS, so using our stopDataNode() method
                    stopDataNode( dn );
                    i++;
                } catch (Exception e) {
                    LOG.info("error shutting down node " + i + ": " + e);
                }
            }
            
            LOG.info("attempting to kill both test nodes");
            // TODO add check to make sure there is enough capacity on these nodes to run test
            stopDataNode(datanode1);
            stopDataNode(datanode2);

            LOG.info("starting up datanode ["+
            datanode1.getHostName()+
            "] and loading it with data");
            startDataNode(datanode1);
            // TODO make an appropriate JMXListener interface
            JMXListenerBean lsnr1 = JMXListenerBean.listenForDataNodeInfo(datanode1);

            // mkdir balancer-temp
            balancerTempDir = makeTempDir();
            // TODO write 2 blocks to file system
            LOG.info("generating filesystem load");
            // TODO spec blocks to generate by blockCount, blockSize, # of writers
            generateFileSystemLoad(2);  // generate 2 blocks of test data

            LOG.info("measure space used on 1st node");
            long usedSpace0 = lsnr1.getDataNodeUsedSpace();
            LOG.info("datanode " + datanode1.getHostName()
                    + " contains " + usedSpace0 + " bytes");

            LOG.info("bring up a 2nd node and run balancer on DFS");
            startDataNode(datanode2);
            runBalancer();

            JMXListenerBean lsnr2 = JMXListenerBean.listenForDataNodeInfo(datanode2);
            LOG.info("measure blocks and files on both nodes, assert these "
                    + "counts are identical pre- and post-balancer run");
            long usedSpace1 = lsnr1.getDataNodeUsedSpace();
            long usedSpace2 = lsnr2.getDataNodeUsedSpace();
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
                startDataNode( dn );
            }
        }
    }

    /** Kill all datanodes but 2, return a list of the reserved datanodes */
    private DNClient[] getReserveDataNodes() {
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
        printDataNodeList(testDNs);

        LOG.info("nodes not used in test");
        printDataNodeList(dieDNs);

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

    private void printDataNodeList(List<DNClient> lis) {
        for (DNClient datanode : lis) {
            LOG.info("\t" + datanode.getHostName());
        }
    }

    private final static String CMD_STOP_DN = "sudo yinst stop hadoop_datanode_admin";
    private void stopDataNode(DNClient dn) {
        String dnHost = dn.getHostName();
        runAndWatch(dnHost, CMD_STOP_DN);
    }
    private final static String CMD_START_DN = "sudo yinst start hadoop_datanode_admin";
    private void startDataNode(DNClient dn) {
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

    static class JMXListenerBean {

        static final String OPTION_REMOTE_PORT       = "-Dcom.sun.management.jmxremote.port";
        static final String HADOOP_JMX_SERVICE_NAME  = "HadoopInfo";
        static final String HADOOP_JMX_INFO_DATANODE = "DataNodeInfo";

        public static JMXListenerBean listenFor(
                AbstractDaemonClient remoteDaemon,
                String typeName)
            throws
                java.io.IOException,
                InstanceNotFoundException {
            String hostName = remoteDaemon.getHostName();
            int portNum = getJmxPortNumber(remoteDaemon);
            ObjectName jmxBeanName = getJmxBeanName(typeName);
            return new JMXListenerBean(hostName, portNum, jmxBeanName);
        }

        public static JMXListenerBean listenForDataNodeInfo(
                AbstractDaemonClient remoteDaemon)
            throws
                java.io.IOException,
                InstanceNotFoundException {
            return listenFor(remoteDaemon, HADOOP_JMX_INFO_DATANODE);
        }

        private static int getJmxPortNumber(AbstractDaemonClient daemon) throws java.io.IOException {
            String hadoopOpts = daemon.getProcessInfo().getEnv().get("HADOOP_OPTS");
            int portNumber = 0;
            boolean found = false;
            String[] options = hadoopOpts.split(" ");
            for(String opt : options) {
                if(opt.startsWith(OPTION_REMOTE_PORT)) {
                    found = true;
                    try {
                        portNumber = Integer.parseInt(opt.split("=")[1]);
                    } catch(NumberFormatException numFmtExc) {
                        throw new IllegalArgumentException("JMX remote port is not an integer");
                    } catch(ArrayIndexOutOfBoundsException outOfBoundsExc) {
                        throw new IllegalArgumentException("JMX remote port not found");
                    }
                }
            }
            if (!found) {
                String errMsg =
                        String.format("Cannot detect JMX remote port for %s daemon on host %s",
                        getDaemonType(daemon),
                        daemon.getHostName());
                throw new IllegalArgumentException(errMsg);
            }
            return portNumber;
        }

        private static String getDaemonType(AbstractDaemonClient daemon) {
            Class daemonClass = daemon.getClass();
            if (daemonClass.equals(DNClient.class))
                return "datanode";
            else if (daemonClass.equals(TTClient.class))
                return "tasktracker";
            else if (daemonClass.equals(NNClient.class))
                return "namenode";
            else if (daemonClass.equals(JTClient.class))
                return "jobtracker";
            else
                return "unknown";
        }

        private MBeanServerConnection establishJmxConnection() {
            MBeanServerConnection conn = null;
            String urlPattern = String.format(
                        "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi",
                        hostName, portNumber );
            try {
                JMXServiceURL url = new JMXServiceURL(urlPattern);
                JMXConnector connector = JMXConnectorFactory.connect(url,null);
                conn = connector.getMBeanServerConnection();
            } catch(java.net.MalformedURLException badURLExc) {
                LOG.debug("bad url: "+urlPattern, badURLExc);
            } catch(java.io.IOException ioExc) {
                LOG.debug("i/o error!", ioExc);
            }
            return conn;
        }

        private static ObjectName getJmxBeanName(String typeName) {
            ObjectName jmxBean = null;
            String jmxRef = String.format(
                    "%s:type=%s",
                    HADOOP_JMX_SERVICE_NAME, typeName);
            try {
                jmxBean = new ObjectName(jmxRef);
            } catch(MalformedObjectNameException badObjNameExc) {
                LOG.debug("bad jmx name: "+jmxRef, badObjNameExc);
            }
            return jmxBean;
        }

        private String hostName;
        private int portNumber;
        private ObjectName beanName;

        private JMXListenerBean(String hostName, int portNumber, ObjectName beanName)
                throws
                IOException,
                InstanceNotFoundException {
            //this.conn = conn;
            this.hostName = hostName;
            this.portNumber = portNumber;
            this.beanName = beanName;
        }

        private Object getAttribute(String attribName)
                throws
                javax.management.AttributeNotFoundException,
                javax.management.InstanceNotFoundException,
                javax.management.ReflectionException,
                javax.management.MBeanException,
                java.io.IOException {

            MBeanServerConnection conn = establishJmxConnection();
            return conn.getAttribute(beanName, attribName);
        }

        private final static String TITLE_UBAR;
        private final static String TOTAL_OBAR;
        static {
            char[] ubar1 = new char[100];
            Arrays.fill(ubar1, '=');
            TITLE_UBAR = new String(ubar1);
            Arrays.fill(ubar1, '-');
            TOTAL_OBAR = new String(ubar1);
        }

        private void printVolInfo(Map volInfoMap) {
            StringBuilder bldr = new StringBuilder();
            if (LOG.isDebugEnabled()) {
                String spaceType = (String)volInfoMap.get("spaceType");
                String spaceTypeHeader = "Space ";
                if(spaceType.startsWith("used")) {
                    spaceTypeHeader += "Used";
                } else {
                    spaceTypeHeader += "Free";
                }
                String titleLine = String.format(
                    "%30s\t%20s\n%30s\t%20s",
                    "Volume", "Space "+spaceType, TITLE_UBAR, TITLE_UBAR);
                bldr.append( titleLine );
                for (Object key : volInfoMap.keySet()) {
                    if ("total".equals(key))
                        continue;

                    Map attrMap = (Map) volInfoMap.get(key);
                    long usedSpace = (Long) attrMap.get(spaceType);
                    bldr.append(String.format("%30s\t%20s",key,usedSpace));
                }
                String totalLine = String.format(
                        "%30s\t%20s\n%30s\t%20s",
                        TOTAL_OBAR, TOTAL_OBAR, "Total", volInfoMap.get("total"));
                bldr.append(totalLine);
                LOG.debug( bldr.toString() );
            }
        }

        public Map processVolInfo(String spaceType)
                throws
                javax.management.AttributeNotFoundException,
                javax.management.InstanceNotFoundException,
                javax.management.ReflectionException,
                javax.management.MBeanException,
                java.io.IOException {

            Object volInfo = getAttribute("VolumeInfo");
            LOG.debug("retrieved volume info object " + volInfo);
            Map info = (Map) JSON.parse(volInfo.toString());
            long total = 0L;
            for (Object key : info.keySet()) {
                Map attrMap = (Map) info.get(key);
                long volAlloc = (Long) attrMap.get(spaceType);
                LOG.info(String.format("volume %s has %d bytes space in use", key, volAlloc));
                total  += volAlloc;
            }
            info.put("total", total);
            info.put("spaceType", spaceType);
            return info;
        }

        public long getDataNodeUsedSpace()
                throws
                javax.management.AttributeNotFoundException,
                javax.management.InstanceNotFoundException,
                javax.management.ReflectionException,
                javax.management.MBeanException,
                java.io.IOException {

            LOG.debug("checking DFS space used on host " + hostName);
            Map volInfoMap = processVolInfo("usedSpace");
            printVolInfo(volInfoMap);
            long totalUsedSpace =  Long.parseLong(volInfoMap.get("total").toString());
            return totalUsedSpace;
        }

        public long getDataNodeFreeSpace()
                throws
                javax.management.AttributeNotFoundException,
                javax.management.InstanceNotFoundException,
                javax.management.ReflectionException,
                javax.management.MBeanException,
                java.io.IOException {

            LOG.debug("checking DFS space free on host " + hostName);
            Map volInfoMap = processVolInfo("freeSpace");
            printVolInfo(volInfoMap);
            long totalFreeSpace = Long.parseLong(volInfoMap.get("total").toString());
            return totalFreeSpace;
        }
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
}
