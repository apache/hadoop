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

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.*;

import java.io.*;
import java.net.*;
import java.util.*;

/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and 
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects 
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of 
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 * @author Mike Cafarella, Tessa MacDuff
 ********************************************************/
class DFSClient implements FSConstants {
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.fs.DFSClient");
    static int MAX_BLOCK_ACQUIRE_FAILURES = 3;
    private static final long DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
    ClientProtocol namenode;
    String localName;
    boolean running = true;
    Random r = new Random();
    String clientName;
    Daemon leaseChecker;
    private Configuration conf;
    private long defaultBlockSize;
    private short defaultReplication;
    
    /**
     * A map from name -> DFSOutputStream of files that are currently being
     * written by this client.
     */
    private TreeMap pendingCreates = new TreeMap();
    
    /**
     * A class to track the list of DFS clients, so that they can be closed
     * on exit.
     * @author Owen O'Malley
     */
    private static class ClientFinalizer extends Thread {
      private List clients = new ArrayList();

      public synchronized void addClient(DFSClient client) {
        clients.add(client);
      }

      public synchronized void run() {
        Iterator itr = clients.iterator();
        while (itr.hasNext()) {
          DFSClient client = (DFSClient) itr.next();
          if (client.running) {
            try {
              client.close();
            } catch (IOException ie) {
              System.err.println("Error closing client");
              ie.printStackTrace();
            }
          }
        }
      }
    }

    // add a cleanup thread
    private static ClientFinalizer clientFinalizer = new ClientFinalizer();
    static {
      Runtime.getRuntime().addShutdownHook(clientFinalizer);
    }

        
    /** 
     * Create a new DFSClient connected to the given namenode server.
     */
    public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf)
    throws IOException {
        this.conf = conf;
        this.namenode = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
            ClientProtocol.versionID, nameNodeAddr, conf);
        try {
            this.localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            this.localName = "";
        }
        String taskId = conf.get("mapred.task.id");
        if (taskId != null) {
            this.clientName = "DFSClient_" + taskId; 
        } else {
            this.clientName = "DFSClient_" + r.nextInt();
        }
        defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
        defaultReplication = (short) conf.getInt("dfs.replication", 3);
        this.leaseChecker = new Daemon(new LeaseChecker());
        this.leaseChecker.start();
    }

    private void checkOpen() throws IOException {
      if (!running) {
        IOException result = new IOException("Filesystem closed");
        throw result;
      }
    }
    
    /**
     * Close the file system, abadoning all of the leases and files being
     * created.
     */
    public void close() throws IOException {
      // synchronize in here so that we don't need to change the API
      synchronized (this) {
        checkOpen();
        synchronized (pendingCreates) {
          Iterator file_itr = pendingCreates.keySet().iterator();
          while (file_itr.hasNext()) {
            String name = (String) file_itr.next();
            try {
              namenode.abandonFileInProgress(name, clientName);
            } catch (IOException ie) {
              System.err.println("Exception abandoning create lock on " + name);
              ie.printStackTrace();
            }
          }
          pendingCreates.clear();
        }
        this.running = false;
        try {
            leaseChecker.join();
        } catch (InterruptedException ie) {
        }
      }
    }

    /**
     * Get the default block size for this cluster
     * @return the default block size in bytes
     */
    public long getDefaultBlockSize() {
      return defaultBlockSize;
    }
    
    public long getBlockSize(Path f) throws IOException {
      // if we already know the answer, use it.
      if (f instanceof DfsPath) {
        return ((DfsPath) f).getBlockSize();
      }
      int retries = 4;
      while (true) {
        try {
          return namenode.getBlockSize(f.toString());
        } catch (IOException ie) {
          LOG.info("Problem getting block size: " + 
                   StringUtils.stringifyException(ie));
          if (--retries == 0) {
            throw ie;
          }
        }
      }
    }
    
    public short getDefaultReplication() {
      return defaultReplication;
    }
    
    /**
     * Get hints about the location of the indicated block(s).  The
     * array returned is as long as there are blocks in the indicated
     * range.  Each block may have one or more locations.
     */
    public String[][] getHints(UTF8 src, long start, long len) throws IOException {
        return namenode.getHints(src.toString(), start, len);
    }

    /**
     * Create an input stream that obtains a nodelist from the
     * namenode, and then reads from all the right places.  Creates
     * inner subclass of InputStream that does the right out-of-band
     * work.
     */
    public FSInputStream open(UTF8 src) throws IOException {
        checkOpen();
        //    Get block info from namenode
        return new DFSInputStream(src.toString());
    }

    /**
     * Create a new dfs file and return an output stream for writing into it. 
     * 
     * @param src stream name
     * @param overwrite do not check for file existence if true
     * @return output stream
     * @throws IOException
     */
    public FSOutputStream create( UTF8 src, 
                                  boolean overwrite
                                ) throws IOException {
      return create( src, overwrite, defaultReplication, defaultBlockSize, null);
    }
    
    /**
     * Create a new dfs file and return an output stream for writing into it
     * with write-progress reporting. 
     * 
     * @param src stream name
     * @param overwrite do not check for file existence if true
     * @return output stream
     * @throws IOException
     */
    public FSOutputStream create( UTF8 src, 
                                  boolean overwrite,
                                  Progressable progress
                                ) throws IOException {
      return create( src, overwrite, defaultReplication, defaultBlockSize, null);
    }
    
    /**
     * Create a new dfs file with the specified block replication 
     * and return an output stream for writing into the file.  
     * 
     * @param src stream name
     * @param overwrite do not check for file existence if true
     * @param replication block replication
     * @return output stream
     * @throws IOException
     */
    public FSOutputStream create( UTF8 src, 
                                  boolean overwrite, 
                                  short replication,
                                  long blockSize
                                ) throws IOException {
      return create(src, overwrite, replication, blockSize, null);
    }

    /**
     * Create a new dfs file with the specified block replication 
     * with write-progress reporting and return an output stream for writing
     * into the file.  
     * 
     * @param src stream name
     * @param overwrite do not check for file existence if true
     * @param replication block replication
     * @return output stream
     * @throws IOException
     */
    public FSOutputStream create( UTF8 src, 
                                  boolean overwrite, 
                                  short replication,
                                  long blockSize,
                                  Progressable progress
                                ) throws IOException {
      checkOpen();
      FSOutputStream result = new DFSOutputStream(src, overwrite, 
                                                  replication, blockSize, progress);
      synchronized (pendingCreates) {
        pendingCreates.put(src.toString(), result);
      }
      return result;
    }
    /**
     * Set replication for an existing file.
     * 
     * @see ClientProtocol#setReplication(String, short)
     * @param replication
     * @throws IOException
     * @return true is successful or false if file does not exist 
     * @author shv
     */
    public boolean setReplication(UTF8 src, 
                                  short replication
                                ) throws IOException {
      return namenode.setReplication(src.toString(), replication);
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean rename(UTF8 src, UTF8 dst) throws IOException {
        checkOpen();
        return namenode.rename(src.toString(), dst.toString());
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean delete(UTF8 src) throws IOException {
        checkOpen();
        return namenode.delete(src.toString());
    }

    /**
     */
    public boolean exists(UTF8 src) throws IOException {
        checkOpen();
        return namenode.exists(src.toString());
    }

    /**
     */
    public boolean isDirectory(UTF8 src) throws IOException {
        checkOpen();
        return namenode.isDir(src.toString());
    }

    /**
     */
    public DFSFileInfo[] listPaths(UTF8 src) throws IOException {
        checkOpen();
        return namenode.getListing(src.toString());
    }

    /**
     */
    public long totalRawCapacity() throws IOException {
        long rawNums[] = namenode.getStats();
        return rawNums[0];
    }

    /**
     */
    public long totalRawUsed() throws IOException {
        long rawNums[] = namenode.getStats();
        return rawNums[1];
    }

    public DatanodeInfo[] datanodeReport() throws IOException {
        return namenode.getDatanodeReport();
    }
    
    /**
     * Enter, leave or get safe mode.
     * See {@link ClientProtocol#setSafeMode(FSConstants.SafeModeAction)} 
     * for more details.
     * 
     * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
     */
    public boolean setSafeMode( SafeModeAction action ) throws IOException {
      return namenode.setSafeMode( action );
    }

    /**
     */
    public boolean mkdirs(UTF8 src) throws IOException {
        checkOpen();
        return namenode.mkdirs(src.toString());
    }

    /**
     */
    public void lock(UTF8 src, boolean exclusive) throws IOException {
        long start = System.currentTimeMillis();
        boolean hasLock = false;
        while (! hasLock) {
            hasLock = namenode.obtainLock(src.toString(), clientName, exclusive);
            if (! hasLock) {
                try {
                    Thread.sleep(400);
                    if (System.currentTimeMillis() - start > 5000) {
                        LOG.info("Waiting to retry lock for " + (System.currentTimeMillis() - start) + " ms.");
                        Thread.sleep(2000);
                    }
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     *
     */
    public void release(UTF8 src) throws IOException {
        boolean hasReleased = false;
        while (! hasReleased) {
            hasReleased = namenode.releaseLock(src.toString(), clientName);
            if (! hasReleased) {
                LOG.info("Could not release.  Retrying...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Pick the best node from which to stream the data.
     * That's the local one, if available.
     */
    private DatanodeInfo bestNode(DatanodeInfo nodes[], TreeSet deadNodes) throws IOException {
        if ((nodes == null) || 
            (nodes.length - deadNodes.size() < 1)) {
            throw new IOException("No live nodes contain current block");
        }
        DatanodeInfo chosenNode = null;
        for (int i = 0; i < nodes.length; i++) {
            if (deadNodes.contains(nodes[i])) {
                continue;
            }
            String nodename = nodes[i].getHost();
            if (localName.equals(nodename)) {
                chosenNode = nodes[i];
                break;
            }
        }
        if (chosenNode == null) {
            do {
                chosenNode = nodes[Math.abs(r.nextInt()) % nodes.length];
            } while (deadNodes.contains(chosenNode));
        }
        return chosenNode;
    }

    /***************************************************************
     * Periodically check in with the namenode and renew all the leases
     * when the lease period is half over.
     ***************************************************************/
    class LeaseChecker implements Runnable {
        /**
         */
        public void run() {
            long lastRenewed = 0;
            while (running) {
                if (System.currentTimeMillis() - lastRenewed > (LEASE_SOFTLIMIT_PERIOD / 2)) {
                    try {
                      if( pendingCreates.size() > 0 )
                        namenode.renewLease(clientName);
                      lastRenewed = System.currentTimeMillis();
                    } catch (IOException ie) {
                      String err = StringUtils.stringifyException(ie);
                      LOG.warn("Problem renewing lease for " + clientName +
                                  ": " + err);
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /** Utility class to encapsulate data node info and its ip address. */
    private static class DNAddrPair {
      DatanodeInfo info;
      InetSocketAddress addr;
      DNAddrPair(DatanodeInfo info, InetSocketAddress addr) {
        this.info = info;
        this.addr = addr;
      }
    }
        
    /****************************************************************
     * DFSInputStream provides bytes from a named file.  It handles 
     * negotiation of the namenode and various datanodes as necessary.
     ****************************************************************/
    class DFSInputStream extends FSInputStream {
        private Socket s = null;
        boolean closed = false;

        private String src;
        private DataInputStream blockStream;
        private Block blocks[] = null;
        private DatanodeInfo nodes[][] = null;
        private long pos = 0;
        private long filelen = 0;
        private long blockEnd = -1;

        /**
         */
        public DFSInputStream(String src) throws IOException {
            this.src = src;
            openInfo();
            this.blockStream = null;
            for (int i = 0; i < blocks.length; i++) {
                this.filelen += blocks[i].getNumBytes();
            }
        }

        /**
         * Grab the open-file info from namenode
         */
        synchronized void openInfo() throws IOException {
            Block oldBlocks[] = this.blocks;

            LocatedBlock results[] = namenode.open(src);            
            Vector blockV = new Vector();
            Vector nodeV = new Vector();
            for (int i = 0; i < results.length; i++) {
                blockV.add(results[i].getBlock());
                nodeV.add(results[i].getLocations());
            }
            Block newBlocks[] = (Block[]) blockV.toArray(new Block[blockV.size()]);

            if (oldBlocks != null) {
                for (int i = 0; i < oldBlocks.length; i++) {
                    if (! oldBlocks[i].equals(newBlocks[i])) {
                        throw new IOException("Blocklist for " + src + " has changed!");
                    }
                }
                if (oldBlocks.length != newBlocks.length) {
                    throw new IOException("Blocklist for " + src + " now has different length");
                }
            }
            this.blocks = newBlocks;
            this.nodes = (DatanodeInfo[][]) nodeV.toArray(new DatanodeInfo[nodeV.size()][]);
        }

        /**
         * Open a DataInputStream to a DataNode so that it can be read from.
         * We get block ID and the IDs of the destinations at startup, from the namenode.
         */
        private synchronized DatanodeInfo blockSeekTo(long target, TreeSet deadNodes) throws IOException {
            if (target >= filelen) {
                throw new IOException("Attempted to read past end of file");
            }

            if (s != null) {
                s.close();
                s = null;
            }

            //
            // Compute desired block
            //
            int targetBlock = -1;
            long targetBlockStart = 0;
            long targetBlockEnd = 0;
            for (int i = 0; i < blocks.length; i++) {
                long blocklen = blocks[i].getNumBytes();
                targetBlockEnd = targetBlockStart + blocklen - 1;

                if (target >= targetBlockStart && target <= targetBlockEnd) {
                    targetBlock = i;
                    break;
                } else {
                    targetBlockStart = targetBlockEnd + 1;                    
                }
            }
            if (targetBlock < 0) {
                throw new IOException("Impossible situation: could not find target position " + target);
            }
            long offsetIntoBlock = target - targetBlockStart;

            //
            // Connect to best DataNode for desired Block, with potential offset
            //
            int failures = 0;
            DatanodeInfo chosenNode = null;
            while (s == null) {
                DNAddrPair retval = chooseDataNode(targetBlock, deadNodes);
                chosenNode = retval.info;
                InetSocketAddress targetAddr = retval.addr;
            
                try {
                    s = new Socket();
                    s.connect(targetAddr, READ_TIMEOUT);
                    s.setSoTimeout(READ_TIMEOUT);

                    //
                    // Xmit header info to datanode
                    //
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                    out.write(OP_READSKIP_BLOCK);
                    blocks[targetBlock].write(out);
                    out.writeLong(offsetIntoBlock);
                    out.flush();

                    //
                    // Get bytes in block, set streams
                    //
                    DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                    long curBlockSize = in.readLong();
                    long amtSkipped = in.readLong();
                    if (curBlockSize != blocks[targetBlock].len) {
                        throw new IOException("Recorded block size is " + blocks[targetBlock].len + ", but datanode reports size of " + curBlockSize);
                    }
                    if (amtSkipped != offsetIntoBlock) {
                        throw new IOException("Asked for offset of " + offsetIntoBlock + ", but only received offset of " + amtSkipped);
                    }

                    this.pos = target;
                    this.blockEnd = targetBlockEnd;
                    this.blockStream = in;
                    return chosenNode;
                } catch (IOException ex) {
                    // Put chosen node into dead list, continue
                    LOG.debug("Failed to connect to " + targetAddr + ":" 
                              + StringUtils.stringifyException(ex));
                    deadNodes.add(chosenNode);
                    if (s != null) {
                        try {
                            s.close();
                        } catch (IOException iex) {
                        }                        
                    }
                    s = null;
                }
            }
            return chosenNode;
        }

        /**
         * Close it down!
         */
        public synchronized void close() throws IOException {
            checkOpen();
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (s != null) {
                blockStream.close();
                s.close();
                s = null;
            }
            super.close();
            closed = true;
        }

        /**
         * Basic read()
         */
        public synchronized int read() throws IOException {
            checkOpen();
            if (closed) {
                throw new IOException("Stream closed");
            }
            int result = -1;
            if (pos < filelen) {
                if (pos > blockEnd) {
                    blockSeekTo(pos, new TreeSet());
                }
                result = blockStream.read();
                if (result >= 0) {
                    pos++;
                }
            }
            return result;
        }

        /**
         * Read the entire buffer.
         */
        public synchronized int read(byte buf[], int off, int len) throws IOException {
            checkOpen();
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (pos < filelen) {
              int retries = 2;
              DatanodeInfo chosenNode = null;
              TreeSet deadNodes = null;
              while (retries > 0) {
                try {
                  if (pos > blockEnd) {
                      if (deadNodes == null) {
                        deadNodes = new TreeSet();
                      }
                      chosenNode = blockSeekTo(pos, deadNodes);
                  }
                  int realLen = Math.min(len, (int) (blockEnd - pos + 1));
                  int result = blockStream.read(buf, off, realLen);
                  if (result >= 0) {
                      pos += result;
                  }
                  return result;
                } catch (IOException e) {
                  LOG.warn("DFS Read: " + StringUtils.stringifyException(e));
                  blockEnd = -1;
                  if (deadNodes == null) { deadNodes = new TreeSet(); }
                  if (chosenNode != null) { deadNodes.add(chosenNode); }
                  if (--retries == 0) {
                    throw e;
                  }
                }
              }
            }
            return -1;
        }

        
        private DNAddrPair chooseDataNode(int blockId, TreeSet deadNodes)
        throws IOException {
          int failures = 0;
          while (true) {
            try {
              DatanodeInfo chosenNode = bestNode(nodes[blockId], deadNodes);
              InetSocketAddress targetAddr = DataNode.createSocketAddr(chosenNode.getName());
              return new DNAddrPair(chosenNode, targetAddr);
            } catch (IOException ie) {
              String blockInfo =
                  blocks[blockId]+" file="+src;
              if (failures >= MAX_BLOCK_ACQUIRE_FAILURES) {
                throw new IOException("Could not obtain block: " + blockInfo);
              }
              if (nodes[blockId] == null || nodes[blockId].length == 0) {
                LOG.info("No node available for block: " + blockInfo);
              }
              LOG.info("Could not obtain block from any node:  " + ie);
              try {
                Thread.sleep(3000);
              } catch (InterruptedException iex) {
              }
              deadNodes.clear();
              openInfo();
              failures++;
              continue;
            }
          }
        } 
        
        private void fetchBlockByteRange(int blockId, long start,
            long end, byte[] buf, int offset) throws IOException {
          //
          // Connect to best DataNode for desired Block, with potential offset
          //
          TreeSet deadNodes = new TreeSet();
          Socket dn = null;
          while (dn == null) {
            DNAddrPair retval = chooseDataNode(blockId, deadNodes);
            DatanodeInfo chosenNode = retval.info;
            InetSocketAddress targetAddr = retval.addr;
            
            try {
              dn = new Socket();
              dn.connect(targetAddr, READ_TIMEOUT);
              dn.setSoTimeout(READ_TIMEOUT);
              
              //
              // Xmit header info to datanode
              //
              DataOutputStream out = new DataOutputStream(new BufferedOutputStream(dn.getOutputStream()));
              out.write(OP_READ_RANGE_BLOCK);
              blocks[blockId].write(out);
              out.writeLong(start);
              out.writeLong(end);
              out.flush();
              
              //
              // Get bytes in block, set streams
              //
              DataInputStream in = new DataInputStream(new BufferedInputStream(dn.getInputStream()));
              long curBlockSize = in.readLong();
              long actualStart = in.readLong();
              long actualEnd = in.readLong();
              if (curBlockSize != blocks[blockId].len) {
                throw new IOException("Recorded block size is " +
                    blocks[blockId].len + ", but datanode reports size of " +
                    curBlockSize);
              }
              if ((actualStart != start) || (actualEnd != end)) {
                throw new IOException("Asked for byte range  " + start +
                    "-" + end + ", but only received range " + actualStart +
                    "-" + actualEnd);
              }
              int nread = in.read(buf, offset, (int)(end - start + 1));
            } catch (IOException ex) {
              // Put chosen node into dead list, continue
              LOG.debug("Failed to connect to " + targetAddr + ":" 
                        + StringUtils.stringifyException(ex));
              deadNodes.add(chosenNode);
              if (dn != null) {
                try {
                  dn.close();
                } catch (IOException iex) {
                }
              }
              dn = null;
            }
          }
        }
        
        public int read(long position, byte[] buf, int off, int len)
        throws IOException {
          // sanity checks
          checkOpen();
          if (closed) {
            throw new IOException("Stream closed");
          }
          if ((position < 0) || (position > filelen)) {
            return -1;
          }
          int realLen = len;
          if ((position + len) > filelen) {
            realLen = (int)(filelen - position);
          }
          // determine the block and byte range within the block
          // corresponding to position and realLen
          int targetBlock = -1;
          long targetStart = 0;
          long targetEnd = 0;
          for (int idx = 0; idx < blocks.length; idx++) {
            long blocklen = blocks[idx].getNumBytes();
            targetEnd = targetStart + blocklen - 1;
            if (position >= targetStart && position <= targetEnd) {
              targetBlock = idx;
              targetStart = position - targetStart;
              targetEnd = Math.min(blocklen, targetStart + realLen) - 1;
              realLen = (int)(targetEnd - targetStart + 1);
              break;
            }
            targetStart += blocklen;
          }
          if (targetBlock < 0) {
            throw new IOException(
                "Impossible situation: could not find target position "+
                position);
          }
          fetchBlockByteRange(targetBlock, targetStart, targetEnd, buf, off);
          return realLen;
        }
        
        /**
         * Seek to a new arbitrary location
         */
        public synchronized void seek(long targetPos) throws IOException {
            if (targetPos > filelen) {
                throw new IOException("Cannot seek after EOF");
            }
            pos = targetPos;
            blockEnd = -1;
        }

        /**
         */
        public synchronized long getPos() throws IOException {
            return pos;
        }

        /**
         */
        public synchronized int available() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            return (int) (filelen - pos);
        }

        /**
         * We definitely don't support marks
         */
        public boolean markSupported() {
            return false;
        }
        public void mark(int readLimit) {
        }
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }
    }

    /****************************************************************
     * DFSOutputStream creates files from a stream of bytes.
     ****************************************************************/
    class DFSOutputStream extends FSOutputStream {
        private Socket s;
        boolean closed = false;

        private byte outBuf[] = new byte[BUFFER_SIZE];
        private int pos = 0;

        private UTF8 src;
        private boolean overwrite;
        private short replication;
        private boolean firstTime = true;
        private DataOutputStream blockStream;
        private DataInputStream blockReplyStream;
        private File backupFile;
        private OutputStream backupStream;
        private Block block;
        private long filePos = 0;
        private int bytesWrittenToBlock = 0;
        private String datanodeName;
        private long blockSize;

        private Progressable progress;
        /**
         * Create a new output stream to the given DataNode.
         */
        public DFSOutputStream(UTF8 src, boolean overwrite, 
                               short replication, long blockSize,
                               Progressable progress
                               ) throws IOException {
            this.src = src;
            this.overwrite = overwrite;
            this.replication = replication;
            this.backupFile = newBackupFile();
            this.blockSize = blockSize;
            this.backupStream = new FileOutputStream(backupFile);
            this.progress = progress;
            if (progress != null) {
                LOG.debug("Set non-null progress callback on DFSOutputStream "+src);
            }
        }

        private File newBackupFile() throws IOException {
          File result = conf.getFile("dfs.client.buffer.dir",
                                     "tmp"+File.separator+
                                     "client-"+Math.abs(r.nextLong()));
          result.deleteOnExit();
          return result;
        }

        /**
         * Open a DataOutputStream to a DataNode so that it can be written to.
         * This happens when a file is created and each time a new block is allocated.
         * Must get block ID and the IDs of the destinations from the namenode.
         */
        private synchronized void nextBlockOutputStream() throws IOException {
            boolean retry = false;
            long startTime = System.currentTimeMillis();
            do {
                retry = false;
                
                LocatedBlock lb;
                if (firstTime) {
                  lb = locateNewBlock();
                } else {
                  lb = locateFollowingBlock(startTime);
                }

                block = lb.getBlock();
                DatanodeInfo nodes[] = lb.getLocations();

                //
                // Connect to first DataNode in the list.  Abort if this fails.
                //
                InetSocketAddress target = DataNode.createSocketAddr(nodes[0].getName());
                try {
                    s = new Socket();
                    s.connect(target, READ_TIMEOUT);
                    s.setSoTimeout(replication * READ_TIMEOUT);
                    datanodeName = nodes[0].getName();
                } catch (IOException ie) {
                    // Connection failed.  Let's wait a little bit and retry
                    try {
                        if (System.currentTimeMillis() - startTime > 5000) {
                            LOG.info("Waiting to find target node: " + target);
                        }
                        Thread.sleep(6000);
                    } catch (InterruptedException iex) {
                    }
                    if (firstTime) {
                        namenode.abandonFileInProgress(src.toString(), 
                                                       clientName);
                    } else {
                        namenode.abandonBlock(block, src.toString());
                    }
                    retry = true;
                    continue;
                }

                //
                // Xmit header info to datanode
                //
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                out.write(OP_WRITE_BLOCK);
                out.writeBoolean(false);
                block.write(out);
                out.writeInt(nodes.length);
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i].write(out);
                }
                out.write(CHUNKED_ENCODING);
                bytesWrittenToBlock = 0;
                blockStream = out;
                blockReplyStream = new DataInputStream(new BufferedInputStream(s.getInputStream()));
            } while (retry);
            firstTime = false;
        }

        private LocatedBlock locateNewBlock() throws IOException {     
          int retries = 3;
          while (true) {
            while (true) {
              try {
                return namenode.create(src.toString(), clientName.toString(),
                    localName, overwrite, replication, blockSize);
              } catch (RemoteException e) {
                if (--retries == 0 || 
                    !AlreadyBeingCreatedException.class.getName().
                        equals(e.getClassName())) {
                  throw e;
                } else {
                  // because failed tasks take upto LEASE_PERIOD to
                  // release their pendingCreates files, if the file
                  // we want to create is already being created, 
                  // wait and try again.
                  LOG.info(StringUtils.stringifyException(e));
                  try {
                    Thread.sleep(LEASE_SOFTLIMIT_PERIOD);
                  } catch (InterruptedException ie) {
                  }
                }
              }
            }
          }
        }
        
        private LocatedBlock locateFollowingBlock(long start
                                                  ) throws IOException {     
          int retries = 5;
          while (true) {
            long localstart = System.currentTimeMillis();
            while (true) {
              try {
                return namenode.addBlock(src.toString(), 
                                         clientName.toString());
              } catch (RemoteException e) {
                if (--retries == 0 || 
                    !NotReplicatedYetException.class.getName().
                        equals(e.getClassName())) {
                  throw e;
                } else {
                  LOG.info(StringUtils.stringifyException(e));
                  if (System.currentTimeMillis() - localstart > 5000) {
                    LOG.info("Waiting for replication for " + 
                             (System.currentTimeMillis() - localstart)/1000 + 
                             " seconds");
                  }
                  try {
                    Thread.sleep(400);
                  } catch (InterruptedException ie) {
                  }
                }                
              }
            }
          } 
        }

        /**
         * We're referring to the file pos here
         */
        public synchronized long getPos() throws IOException {
            return filePos;
        }
			
        /**
         * Writes the specified byte to this output stream.
         */
        public synchronized void write(int b) throws IOException {
            checkOpen();
            if (closed) {
                throw new IOException("Stream closed");
            }

            if ((bytesWrittenToBlock + pos == blockSize) ||
                (pos >= BUFFER_SIZE)) {
                flush();
            }
            outBuf[pos++] = (byte) b;
            filePos++;
        }

        /**
         * Writes the specified bytes to this output stream.
         */
      public synchronized void write(byte b[], int off, int len)
        throws IOException {
            checkOpen();
            if (closed) {
                throw new IOException("Stream closed");
            }
            while (len > 0) {
              int remaining = Math.min(BUFFER_SIZE - pos,
                  (int)((blockSize - bytesWrittenToBlock) - pos));
              int toWrite = Math.min(remaining, len);
              System.arraycopy(b, off, outBuf, pos, toWrite);
              pos += toWrite;
              off += toWrite;
              len -= toWrite;
              filePos += toWrite;

              if ((bytesWrittenToBlock + pos >= blockSize) ||
                  (pos == BUFFER_SIZE)) {
                flush();
              }
            }
        }

        /**
         * Flush the buffer, getting a stream to a new block if necessary.
         */
        public synchronized void flush() throws IOException {
            checkOpen();
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (bytesWrittenToBlock + pos >= blockSize) {
                flushData((int) blockSize - bytesWrittenToBlock);
            }
            if (bytesWrittenToBlock == blockSize) {
                endBlock();
            }
            flushData(pos);
        }

        /**
         * Actually flush the accumulated bytes to the remote node,
         * but no more bytes than the indicated number.
         */
        private synchronized void flushData(int maxPos) throws IOException {
            int workingPos = Math.min(pos, maxPos);
            
            if (workingPos > 0) {
                //
                // To the local block backup, write just the bytes
                //
                backupStream.write(outBuf, 0, workingPos);

                //
                // Track position
                //
                bytesWrittenToBlock += workingPos;
                System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
                pos -= workingPos;
            }
        }

        /**
         * We're done writing to the current block.
         */
        private synchronized void endBlock() throws IOException {
            //
            // Done with local copy
            //
            backupStream.close();

            //
            // Send it to datanode
            //
            boolean sentOk = false;
            int remainingAttempts = 
               conf.getInt("dfs.client.block.write.retries", 3);
            while (!sentOk) {
                nextBlockOutputStream();
                InputStream in = new FileInputStream(backupFile);
                try {
                    byte buf[] = new byte[BUFFER_SIZE];
                    int bytesRead = in.read(buf);
                    while (bytesRead > 0) {
                        blockStream.writeLong((long) bytesRead);
                        blockStream.write(buf, 0, bytesRead);
                        if (progress != null) { progress.progress(); }
                        bytesRead = in.read(buf);
                    }
                    internalClose();
                    sentOk = true;
                } catch (IOException ie) {
                    handleSocketException(ie);
                    remainingAttempts -= 1;
                    if (remainingAttempts == 0) {
                      throw ie;
                    }
                } finally {
                  in.close();
                }
            }

            //
            // Delete local backup, start new one
            //
            backupFile.delete();
            backupFile = newBackupFile();
            backupStream = new FileOutputStream(backupFile);
            bytesWrittenToBlock = 0;
        }

        /**
         * Close down stream to remote datanode.
         */
        private synchronized void internalClose() throws IOException {
          try {
            blockStream.writeLong(0);
            blockStream.flush();

            long complete = blockReplyStream.readLong();
            if (complete != WRITE_COMPLETE) {
                LOG.info("Did not receive WRITE_COMPLETE flag: " + complete);
                throw new IOException("Did not receive WRITE_COMPLETE_FLAG: " + complete);
            }
          } catch (IOException ie) {
            throw (IOException)
                  new IOException("failure closing block of file " +
                                  src.toString() + " to node " +
                                  (datanodeName == null ? "?" : datanodeName)
                                 ).initCause(ie);
          }
                    
            LocatedBlock lb = new LocatedBlock();
            lb.readFields(blockReplyStream);
            namenode.reportWrittenBlock(lb);

            s.close();
            s = null;
        }

        private void handleSocketException(IOException ie) throws IOException {
          LOG.warn("Error while writing.", ie);
          try {
            if (s != null) {
              s.close();
              s = null;
            }
          } catch (IOException ie2) {
            LOG.warn("Error closing socket.", ie2);
          }
          namenode.abandonBlock(block, src.toString());
        }

        /**
         * Closes this output stream and releases any system 
         * resources associated with this stream.
         */
        public synchronized void close() throws IOException {
          checkOpen();
          if (closed) {
              throw new IOException("Stream closed");
          }
          
          try {
            flush();
            if (filePos == 0 || bytesWrittenToBlock != 0) {
              try {
                endBlock();
              } catch (IOException e) {
                namenode.abandonFileInProgress(src.toString(), clientName);
                throw e;
              }
            }
            
            backupStream.close();
            backupFile.delete();

            if (s != null) {
                s.close();
                s = null;
            }
            super.close();

            long localstart = System.currentTimeMillis();
            boolean fileComplete = false;
            while (! fileComplete) {
              fileComplete = namenode.complete(src.toString(), clientName.toString());
              if (!fileComplete) {
                try {
                  Thread.sleep(400);
                  if (System.currentTimeMillis() - localstart > 5000) {
                    LOG.info("Could not complete file, retrying...");
                  }
                } catch (InterruptedException ie) {
                }
              }
            }
            closed = true;
          } finally {
            synchronized (pendingCreates) {
              pendingCreates.remove(src.toString());
            }
          }
        }
    }
}
