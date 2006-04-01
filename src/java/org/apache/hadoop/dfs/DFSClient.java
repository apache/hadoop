/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

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
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.fs.DFSClient");
    static int MAX_BLOCK_ACQUIRE_FAILURES = 3;
    ClientProtocol namenode;
    String localName;
    boolean running = true;
    Random r = new Random();
    String clientName;
    Daemon leaseChecker;
    private Configuration conf;

    /** 
     * Create a new DFSClient connected to the given namenode server.
     */
    public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf) {
        this.conf = conf;
        this.namenode = (ClientProtocol) RPC.getProxy(ClientProtocol.class, nameNodeAddr, conf);
        try {
            this.localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            this.localName = "";
        }
        this.clientName = "DFSClient_" + r.nextInt();
        this.leaseChecker = new Daemon(new LeaseChecker());
        this.leaseChecker.start();
    }

    /**
     */
    public void close() throws IOException {
        this.running = false;
        try {
            leaseChecker.join();
        } catch (InterruptedException ie) {
        }
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
        // Get block info from namenode
        return new DFSInputStream(src.toString());
    }

    public FSOutputStream create(UTF8 src, boolean overwrite) throws IOException {
        return new DFSOutputStream(src, overwrite);
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean rename(UTF8 src, UTF8 dst) throws IOException {
        return namenode.rename(src.toString(), dst.toString());
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean delete(UTF8 src) throws IOException {
        return namenode.delete(src.toString());
    }

    /**
     */
    public boolean exists(UTF8 src) throws IOException {
        return namenode.exists(src.toString());
    }

    /**
     */
    public boolean isDirectory(UTF8 src) throws IOException {
        return namenode.isDir(src.toString());
    }

    /**
     */
    public DFSFileInfo[] listFiles(UTF8 src) throws IOException {
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
     */
    public boolean mkdirs(UTF8 src) throws IOException {
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
            String nodename = nodes[i].getName().toString();
            int colon = nodename.indexOf(':');
            if (colon >= 0) {
                nodename = nodename.substring(0, colon);
            }
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
                if (System.currentTimeMillis() - lastRenewed > (LEASE_PERIOD / 2)) {
                    try {
                        namenode.renewLease(clientName);
                        lastRenewed = System.currentTimeMillis();
                    } catch (IOException ie) {
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
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
        void openInfo() throws IOException {
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
        private synchronized void blockSeekTo(long target) throws IOException {
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
            InetSocketAddress targetAddr = null;
            TreeSet deadNodes = new TreeSet();
            while (s == null) {
                DatanodeInfo chosenNode;

                try {
                    chosenNode = bestNode(nodes[targetBlock], deadNodes);
                    targetAddr = DataNode.createSocketAddr(chosenNode.getName().toString());
                } catch (IOException ie) {
                    String blockInfo =
                      blocks[targetBlock]+" file="+src+" offset="+target;
                    if (failures >= MAX_BLOCK_ACQUIRE_FAILURES) {
                        throw new IOException("Could not obtain block: " + blockInfo);
                    }
                    if (nodes[targetBlock] == null || nodes[targetBlock].length == 0) {
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
                } catch (IOException ex) {
                    // Put chosen node into dead list, continue
                    LOG.info("Failed to connect to " + targetAddr + ":" + ex);
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
        }

        /**
         * Close it down!
         */
        public synchronized void close() throws IOException {
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
            if (closed) {
                throw new IOException("Stream closed");
            }
            int result = -1;
            if (pos < filelen) {
                if (pos > blockEnd) {
                    blockSeekTo(pos);
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
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (pos < filelen) {
                if (pos > blockEnd) {
                    blockSeekTo(pos);
                }
                int result = blockStream.read(buf, off, Math.min(len, (int) (blockEnd - pos + 1)));
                if (result >= 0) {
                    pos += result;
                }
                return result;
            }
            return -1;
        }

        /**
         * Seek to a new arbitrary location
         */
        public synchronized void seek(long targetPos) throws IOException {
            if (targetPos >= filelen) {
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
        private boolean firstTime = true;
        private DataOutputStream blockStream;
        private DataInputStream blockReplyStream;
        private File backupFile;
        private OutputStream backupStream;
        private Block block;
        private long filePos = 0;
        private int bytesWrittenToBlock = 0;

        /**
         * Create a new output stream to the given DataNode.
         */
        public DFSOutputStream(UTF8 src, boolean overwrite) throws IOException {
            this.src = src;
            this.overwrite = overwrite;
            this.backupFile = newBackupFile();
            this.backupStream = new FileOutputStream(backupFile);
        }

        private File newBackupFile() throws IOException {
          File result = conf.getFile("dfs.data.dir",
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
            long start = System.currentTimeMillis();
            do {
                retry = false;
                
                long localstart = System.currentTimeMillis();
                boolean blockComplete = false;
                LocatedBlock lb = null;                
                while (! blockComplete) {
                    if (firstTime) {
                        lb = namenode.create(src.toString(), clientName.toString(), localName, overwrite);
                    } else {
                        lb = namenode.addBlock(src.toString(), localName);
                    }

                    if (lb == null) {
                        try {
                            Thread.sleep(400);
                            if (System.currentTimeMillis() - localstart > 5000) {
                                LOG.info("Waiting to find new output block node for " + (System.currentTimeMillis() - start) + "ms");
                            }
                        } catch (InterruptedException ie) {
                        }
                    } else {
                        blockComplete = true;
                    }
                }

                block = lb.getBlock();
                DatanodeInfo nodes[] = lb.getLocations();

                //
                // Connect to first DataNode in the list.  Abort if this fails.
                //
                InetSocketAddress target = DataNode.createSocketAddr(nodes[0].getName().toString());
                try {
                    s = new Socket();
                    s.connect(target, READ_TIMEOUT);
                    s.setSoTimeout(READ_TIMEOUT);
                } catch (IOException ie) {
                    // Connection failed.  Let's wait a little bit and retry
                    try {
                        if (System.currentTimeMillis() - start > 5000) {
                            LOG.info("Waiting to find target node: " + target);
                        }
                        Thread.sleep(6000);
                    } catch (InterruptedException iex) {
                    }
                    if (firstTime) {
                        namenode.abandonFileInProgress(src.toString());
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
            if (closed) {
                throw new IOException("Stream closed");
            }

            if ((bytesWrittenToBlock + pos == BLOCK_SIZE) ||
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
            if (closed) {
                throw new IOException("Stream closed");
            }
            while (len > 0) {
              int remaining = BUFFER_SIZE - pos;
              int toWrite = Math.min(remaining, len);
              System.arraycopy(b, off, outBuf, pos, toWrite);
              pos += toWrite;
              off += toWrite;
              len -= toWrite;
              filePos += toWrite;

              if ((bytesWrittenToBlock + pos >= BLOCK_SIZE) ||
                  (pos == BUFFER_SIZE)) {
                flush();
              }
            }
        }

        /**
         * Flush the buffer, getting a stream to a new block if necessary.
         */
        public synchronized void flush() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (bytesWrittenToBlock + pos >= BLOCK_SIZE) {
                flushData(BLOCK_SIZE - bytesWrittenToBlock);
            }
            if (bytesWrittenToBlock == BLOCK_SIZE) {
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
            boolean mustRecover = true;
            while (mustRecover) {
                nextBlockOutputStream();
                InputStream in = new FileInputStream(backupFile);
                try {
                    byte buf[] = new byte[BUFFER_SIZE];
                    int bytesRead = in.read(buf);
                    while (bytesRead > 0) {
                        blockStream.writeLong((long) bytesRead);
                        blockStream.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                    internalClose();
                    mustRecover = false;
                } catch (IOException ie) {
                    handleSocketException(ie);
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
            blockStream.writeLong(0);
            blockStream.flush();

            long complete = blockReplyStream.readLong();
            if (complete != WRITE_COMPLETE) {
                LOG.info("Did not receive WRITE_COMPLETE flag: " + complete);
                throw new IOException("Did not receive WRITE_COMPLETE_FLAG: " + complete);
            }
                    
            LocatedBlock lb = new LocatedBlock();
            lb.readFields(blockReplyStream);
            namenode.reportWrittenBlock(lb);

            s.close();
            s = null;
        }

        private void handleSocketException(IOException ie) throws IOException {
          LOG.log(Level.WARNING, "Error while writing.", ie);
          try {
            if (s != null) {
              s.close();
              s = null;
            }
          } catch (IOException ie2) {
            LOG.log(Level.WARNING, "Error closing socket.", ie2);
          }
          namenode.abandonBlock(block, src.toString());
        }

        /**
         * Closes this output stream and releases any system 
         * resources associated with this stream.
         */
        public synchronized void close() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            flush();
            if (filePos == 0 || bytesWrittenToBlock != 0) {
              try {
                endBlock();
              } catch (IOException e) {
                namenode.abandonFileInProgress(src.toString());
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
        }
    }
}
