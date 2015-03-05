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

package org.apache.hadoop.ha;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.util.Time;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactoryAccessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Copy-paste of ClientBase from ZooKeeper, but without any of the
 * JMXEnv verification. There seems to be a bug ZOOKEEPER-1438
 * which causes spurious failures in the JMXEnv verification when
 * we run these tests with the upstream ClientBase.
 */
public abstract class ClientBaseWithFixes extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(ClientBaseWithFixes.class);

    public static int CONNECTION_TIMEOUT = 30000;
    static final File BASETEST =
        new File(System.getProperty("test.build.data", "build"));

    protected final String hostPort = initHostPort();
    protected int maxCnxns = 0;
    protected ServerCnxnFactory serverFactory = null;
    protected File tmpDir = null;
    
    long initialFdCount;
    
    /**
     * In general don't use this. Only use in the special case that you
     * want to ignore results (for whatever reason) in your test. Don't
     * use empty watchers in real code!
     *
     */
    protected class NullWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) { /* nada */ }
    }

    protected static class CountdownWatcher implements Watcher {
        // XXX this doesn't need to be volatile! (Should probably be final)
        volatile CountDownLatch clientConnected;
        volatile boolean connected;
        protected ZooKeeper client;

        public void initializeWatchedClient(ZooKeeper zk) {
            if (client != null) {
                throw new RuntimeException("Watched Client was already set");
            }
            client = zk;
        }

        public CountdownWatcher() {
            reset();
        }
        synchronized public void reset() {
            clientConnected = new CountDownLatch(1);
            connected = false;
        }
        @Override
        synchronized public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected ||
                event.getState() == KeeperState.ConnectedReadOnly) {
                connected = true;
                notifyAll();
                clientConnected.countDown();
            } else {
                connected = false;
                notifyAll();
            }
        }
        synchronized boolean isConnected() {
            return connected;
        }
        @VisibleForTesting
        public synchronized void waitForConnected(long timeout)
            throws InterruptedException, TimeoutException {
            long expire = Time.now() + timeout;
            long left = timeout;
            while(!connected && left > 0) {
                wait(left);
                left = expire - Time.now();
            }
            if (!connected) {
                throw new TimeoutException("Did not connect");

            }
        }
        @VisibleForTesting
        public synchronized void waitForDisconnected(long timeout)
            throws InterruptedException, TimeoutException {
            long expire = Time.now() + timeout;
            long left = timeout;
            while(connected && left > 0) {
                wait(left);
                left = expire - Time.now();
            }
            if (connected) {
                throw new TimeoutException("Did not disconnect");

            }
        }
    }

    protected TestableZooKeeper createClient()
        throws IOException, InterruptedException
    {
        return createClient(hostPort);
    }

    protected TestableZooKeeper createClient(String hp)
        throws IOException, InterruptedException
    {
        CountdownWatcher watcher = new CountdownWatcher();
        return createClient(watcher, hp);
    }

    private LinkedList<ZooKeeper> allClients;
    private boolean allClientsSetup = false;

    private RandomAccessFile portNumLockFile;

    private File portNumFile;

    protected TestableZooKeeper createClient(CountdownWatcher watcher, String hp)
        throws IOException, InterruptedException
    {
        return createClient(watcher, hp, CONNECTION_TIMEOUT);
    }

    protected TestableZooKeeper createClient(CountdownWatcher watcher,
            String hp, int timeout)
        throws IOException, InterruptedException
    {
        watcher.reset();
        TestableZooKeeper zk = new TestableZooKeeper(hp, timeout, watcher);
        if (!watcher.clientConnected.await(timeout, TimeUnit.MILLISECONDS))
        {
            Assert.fail("Unable to connect to server");
        }
        synchronized(this) {
            if (!allClientsSetup) {
                LOG.error("allClients never setup");
                Assert.fail("allClients never setup");
            }
            if (allClients != null) {
                allClients.add(zk);
            } else {
                // test done - close the zk, not needed
                zk.close();
            }
        }
        watcher.initializeWatchedClient(zk);
        return zk;
    }

    public static class HostPort {
        String host;
        int port;
        public HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }
    public static List<HostPort> parseHostPortList(String hplist) {
        ArrayList<HostPort> alist = new ArrayList<HostPort>();
        for (String hp: hplist.split(",")) {
            int idx = hp.lastIndexOf(':');
            String host = hp.substring(0, idx);
            int port;
            try {
                port = Integer.parseInt(hp.substring(idx + 1));
            } catch(RuntimeException e) {
                throw new RuntimeException("Problem parsing " + hp + e.toString());
            }
            alist.add(new HostPort(host,port));
        }
        return alist;
    }

    /**
     * Send the 4letterword
     * @param host the destination host
     * @param port the destination port
     * @param cmd the 4letterword
     * @return
     * @throws IOException
     */
    public static String send4LetterWord(String host, int port, String cmd)
        throws IOException
    {
        LOG.info("connecting to " + host + " " + port);
        Socket sock = new Socket(host, port);
        BufferedReader reader = null;
        try {
            OutputStream outstream = sock.getOutputStream();
            outstream.write(cmd.getBytes());
            outstream.flush();
            // this replicates NC - close the output stream before reading
            sock.shutdownOutput();

            reader =
                new BufferedReader(
                        new InputStreamReader(sock.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        } finally {
            sock.close();
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static boolean waitForServerUp(String hp, long timeout) {
        long start = Time.now();
        while (true) {
            try {
                // if there are multiple hostports, just take the first one
                HostPort hpobj = parseHostPortList(hp).get(0);
                String result = send4LetterWord(hpobj.host, hpobj.port, "stat");
                if (result.startsWith("Zookeeper version:") &&
                        !result.contains("READ-ONLY")) {
                    return true;
                }
            } catch (IOException e) {
                // ignore as this is expected
                LOG.info("server " + hp + " not up " + e);
            }

            if (Time.now() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }
    public static boolean waitForServerDown(String hp, long timeout) {
        long start = Time.now();
        while (true) {
            try {
                HostPort hpobj = parseHostPortList(hp).get(0);
                send4LetterWord(hpobj.host, hpobj.port, "stat");
            } catch (IOException e) {
                return true;
            }

            if (Time.now() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

    public static File createTmpDir() throws IOException {
        return createTmpDir(BASETEST);
    }
    static File createTmpDir(File parentDir) throws IOException {
        File tmpFile = File.createTempFile("test", ".junit", parentDir);
        // don't delete tmpFile - this ensures we don't attempt to create
        // a tmpDir with a duplicate name
        File tmpDir = new File(tmpFile + ".dir");
        Assert.assertFalse(tmpDir.exists()); // never true if tmpfile does it's job
        Assert.assertTrue(tmpDir.mkdirs());

        return tmpDir;
    }

    private static int getPort(String hostPort) {
        String[] split = hostPort.split(":");
        String portstr = split[split.length-1];
        String[] pc = portstr.split("/");
        if (pc.length > 1) {
            portstr = pc[0];
        }
        return Integer.parseInt(portstr);
    }

    static ServerCnxnFactory createNewServerInstance(File dataDir,
            ServerCnxnFactory factory, String hostPort, int maxCnxns)
        throws IOException, InterruptedException
    {
        ZooKeeperServer zks = new ZooKeeperServer(dataDir, dataDir, 3000);
        final int PORT = getPort(hostPort);
        if (factory == null) {
            factory = ServerCnxnFactory.createFactory(PORT, maxCnxns);
        }
        factory.startup(zks);
        Assert.assertTrue("waiting for server up",
                   ClientBaseWithFixes.waitForServerUp("127.0.0.1:" + PORT,
                                              CONNECTION_TIMEOUT));

        return factory;
    }

    static void shutdownServerInstance(ServerCnxnFactory factory,
            String hostPort)
    {
        if (factory != null) {
            ZKDatabase zkDb;
            {
                ZooKeeperServer zs = getServer(factory);
        
                zkDb = zs.getZKDatabase();
            }
            factory.shutdown();
            try {
                zkDb.close();
            } catch (IOException ie) {
                LOG.warn("Error closing logs ", ie);
            }
            final int PORT = getPort(hostPort);

            Assert.assertTrue("waiting for server down",
                       ClientBaseWithFixes.waitForServerDown("127.0.0.1:" + PORT,
                                                    CONNECTION_TIMEOUT));
        }
    }

    /**
     * Test specific setup
     */
    public static void setupTestEnv() {
        // during the tests we run with 100K prealloc in the logs.
        // on windows systems prealloc of 64M was seen to take ~15seconds
        // resulting in test Assert.failure (client timeout on first session).
        // set env and directly in order to handle static init/gc issues
        System.setProperty("zookeeper.preAllocSize", "100");
        FileTxnLog.setPreallocSize(100 * 1024);
    }

    protected void setUpAll() throws Exception {
        allClients = new LinkedList<ZooKeeper>();
        allClientsSetup = true;
    }

    @Before
    public void setUp() throws Exception {
        BASETEST.mkdirs();

        setupTestEnv();

        setUpAll();

        tmpDir = createTmpDir(BASETEST);

        startServer();

        LOG.info("Client test setup finished");
    }

    private String initHostPort() {
        BASETEST.mkdirs();
        int port;
        for (;;) {
            port = PortAssignment.unique();
            FileLock lock = null;
            portNumLockFile = null;
            try {
                try {
                    portNumFile = new File(BASETEST, port + ".lock");
                    portNumLockFile = new RandomAccessFile(portNumFile, "rw");
                    try {
                        lock = portNumLockFile.getChannel().tryLock();
                    } catch (OverlappingFileLockException e) {
                        continue;
                    }
                } finally {
                    if (lock != null)
                        break;
                    if (portNumLockFile != null)
                        portNumLockFile.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return "127.0.0.1:" + port;
    }

    protected void startServer() throws Exception {
        LOG.info("STARTING server");
        serverFactory = createNewServerInstance(tmpDir, serverFactory, hostPort, maxCnxns);
    }

    protected void stopServer() throws Exception {
        LOG.info("STOPPING server");
        shutdownServerInstance(serverFactory, hostPort);
        serverFactory = null;
    }


    protected static ZooKeeperServer getServer(ServerCnxnFactory fac) {
        ZooKeeperServer zs = ServerCnxnFactoryAccessor.getZkServer(fac);

        return zs;
    }

    protected void tearDownAll() throws Exception {
        synchronized (this) {
            if (allClients != null) for (ZooKeeper zk : allClients) {
                try {
                    if (zk != null)
                        zk.close();
                } catch (InterruptedException e) {
                    LOG.warn("ignoring interrupt", e);
                }
            }
            allClients = null;
        }
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("tearDown starting");

        tearDownAll();

        stopServer();

        portNumLockFile.close();
        portNumFile.delete();
        
        if (tmpDir != null) {
            Assert.assertTrue("delete " + tmpDir.toString(), recursiveDelete(tmpDir));
        }

        // This has to be set to null when the same instance of this class is reused between test cases
        serverFactory = null;
    }

    public static boolean recursiveDelete(File d) {
        if (d.isDirectory()) {
            File children[] = d.listFiles();
            for (File f : children) {
                Assert.assertTrue("delete " + f.toString(), recursiveDelete(f));
            }
        }
        return d.delete();
    }
}
