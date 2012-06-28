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

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.raid.Decoder;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.ReedSolomonDecoder;
import org.apache.hadoop.raid.XORDecoder;
import org.apache.hadoop.raid.protocol.PolicyInfo.ErasureCodeType;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This is an implementation of the Hadoop  RAID Filesystem. This FileSystem 
 * wraps an instance of the DistributedFileSystem.
 * If a file is corrupted, this FileSystem uses the parity blocks to 
 * regenerate the bad block.
 */

public class DistributedRaidFileSystem extends FilterFileSystem {

  // these are alternate locations that can be used for read-only access
  DecodeInfo[] alternates;
  Configuration conf;
  int stripeLength;

  DistributedRaidFileSystem() throws IOException {
  }

  DistributedRaidFileSystem(FileSystem fs) throws IOException {
    super(fs);
    alternates = null;
    stripeLength = 0;
  }

  // Information required for decoding a source file
  static private class DecodeInfo {
    final Path destPath;
    final ErasureCodeType type;
    final Configuration conf;
    final int stripeLength;
    private DecodeInfo(Configuration conf, ErasureCodeType type, Path destPath) {
      this.conf = conf;
      this.type = type;
      this.destPath = destPath;
      this.stripeLength = RaidNode.getStripeLength(conf);
    }

    Decoder createDecoder() {
      if (this.type == ErasureCodeType.XOR) {
        return new XORDecoder(conf, stripeLength);
      } else if (this.type == ErasureCodeType.RS) {
        return new ReedSolomonDecoder(conf, stripeLength,
                              RaidNode.rsParityLength(conf));
      }
      return null;
    }
  }

  /* Initialize a Raid FileSystem
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    this.conf = conf;

    Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
        DistributedFileSystem.class);
    if (clazz == null) {
      throw new IOException("No FileSystem for fs.raid.underlyingfs.impl.");
    }
    
    this.fs = (FileSystem)ReflectionUtils.newInstance(clazz, null); 
    super.initialize(name, conf);
    
    // find stripe length configured
    stripeLength = RaidNode.getStripeLength(conf);
    if (stripeLength == 0) {
      LOG.info("dfs.raid.stripeLength is incorrectly defined to be " + 
               stripeLength + " Ignoring...");
      return;
    }

    // Put XOR and RS in alternates
    alternates= new DecodeInfo[2];
    Path xorPath = RaidNode.xorDestinationPath(conf, fs);
    alternates[0] = new DecodeInfo(conf, ErasureCodeType.XOR, xorPath);
    Path rsPath = RaidNode.rsDestinationPath(conf, fs);
    alternates[1] = new DecodeInfo(conf, ErasureCodeType.RS, rsPath);
  }

  /*
   * Returns the underlying filesystem
   */
  public FileSystem getFileSystem() throws IOException {
    return fs;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    ExtFSDataInputStream fd = new ExtFSDataInputStream(conf, this, alternates, f,
                                                       stripeLength, bufferSize);
    return fd;
  }

  public void close() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch(IOException ie) {
        //this might already be closed, ignore
      }
    }
    super.close();
  }

  /**
   * Layered filesystem input stream. This input stream tries reading
   * from alternate locations if it encoumters read errors in the primary location.
   */
  private static class ExtFSDataInputStream extends FSDataInputStream {

    private static class UnderlyingBlock {
      // File that holds this block. Need not be the same as outer file.
      public Path path;
      // Offset within path where this block starts.
      public long actualFileOffset;
      // Offset within the outer file where this block starts.
      public long originalFileOffset;
      // Length of the block (length <= blk sz of outer file).
      public long length;
      public UnderlyingBlock(Path path, long actualFileOffset,
          long originalFileOffset, long length) {
        this.path = path;
        this.actualFileOffset = actualFileOffset;
        this.originalFileOffset = originalFileOffset;
        this.length = length;
      }
    }

    /**
     * Create an input stream that wraps all the reads/positions/seeking.
     */
    private static class ExtFsInputStream extends FSInputStream {

      // Extents of "good" underlying data that can be read.
      private UnderlyingBlock[] underlyingBlocks;
      private long currentOffset;
      private FSDataInputStream currentStream;
      private UnderlyingBlock currentBlock;
      private byte[] oneBytebuff = new byte[1];
      private int nextLocation;
      private DistributedRaidFileSystem lfs;
      private Path path;
      private FileStatus stat;
      private final DecodeInfo[] alternates;
      private final int buffersize;
      private final Configuration conf;
      private final int stripeLength;

      ExtFsInputStream(Configuration conf, DistributedRaidFileSystem lfs,
          DecodeInfo[] alternates, Path path, int stripeLength, int buffersize)
          throws IOException {
        this.path = path;
        this.nextLocation = 0;
        // Construct array of blocks in file.
        this.stat = lfs.getFileStatus(path);
        long numBlocks = (this.stat.getLen() % this.stat.getBlockSize() == 0) ?
                        this.stat.getLen() / this.stat.getBlockSize() :
                        1 + this.stat.getLen() / this.stat.getBlockSize();
        this.underlyingBlocks = new UnderlyingBlock[(int)numBlocks];
        for (int i = 0; i < numBlocks; i++) {
          long actualFileOffset = i * stat.getBlockSize();
          long originalFileOffset = i * stat.getBlockSize();
          long length = Math.min(
            stat.getBlockSize(), stat.getLen() - originalFileOffset);
          this.underlyingBlocks[i] = new UnderlyingBlock(
            path, actualFileOffset, originalFileOffset, length);
        }
        this.currentOffset = 0;
        this.currentBlock = null;
        this.alternates = alternates;
        this.buffersize = buffersize;
        this.conf = conf;
        this.lfs = lfs;
        this.stripeLength = stripeLength;
        // Open a stream to the first block.
        openCurrentStream();
      }

      private void closeCurrentStream() throws IOException {
        if (currentStream != null) {
          currentStream.close();
          currentStream = null;
        }
      }

      /**
       * Open a stream to the file containing the current block
       * and seek to the appropriate offset
       */
      private void openCurrentStream() throws IOException {
        int blockIdx = (int)(currentOffset/stat.getBlockSize());
        UnderlyingBlock block = underlyingBlocks[blockIdx];
        // If the current path is the same as we want.
        if (currentBlock == block ||
           currentBlock != null && currentBlock.path == block.path) {
          // If we have a valid stream, nothing to do.
          if (currentStream != null) {
            currentBlock = block;
            return;
          }
        } else {
          closeCurrentStream();
        }
        currentBlock = block;
        currentStream = lfs.fs.open(currentBlock.path, buffersize);
        long offset = block.actualFileOffset +
          (currentOffset - block.originalFileOffset);
        currentStream.seek(offset);
      }

      /**
       * Returns the number of bytes available in the current block.
       */
      private int blockAvailable() {
        return (int) (currentBlock.length -
                (currentOffset - currentBlock.originalFileOffset));
      }

      @Override
      public synchronized int available() throws IOException {
        // Application should not assume that any bytes are buffered here.
        nextLocation = 0;
        return Math.min(blockAvailable(), currentStream.available());
      }

      @Override
      public synchronized  void close() throws IOException {
        closeCurrentStream();
        super.close();
      }

      @Override
      public boolean markSupported() { return false; }

      @Override
      public void mark(int readLimit) {
        // Mark and reset are not supported.
        nextLocation = 0;
      }

      @Override
      public void reset() throws IOException {
        // Mark and reset are not supported.
        nextLocation = 0;
      }

      @Override
      public synchronized int read() throws IOException {
        int value = read(oneBytebuff);
        if (value < 0) {
          return value;
        } else {
          return oneBytebuff[0];
        }
      }

      @Override
      public synchronized int read(byte[] b) throws IOException {
        int value = read(b, 0, b.length);
        nextLocation = 0;
        return value;
      }

      @Override
      public synchronized int read(byte[] b, int offset, int len) 
        throws IOException {
        while (true) {
          openCurrentStream();
          try{
            int limit = Math.min(blockAvailable(), len);
            int value = currentStream.read(b, offset, limit);
            currentOffset += value;
            nextLocation = 0;
            return value;
          } catch (BlockMissingException e) {
            setAlternateLocations(e, currentOffset);
          } catch (ChecksumException e) {
            setAlternateLocations(e, currentOffset);
          }
        }
      }
      
      @Override
      public synchronized int read(long position, byte[] b, int offset, int len) 
        throws IOException {
        long oldPos = currentOffset;
        seek(position);
        try {
          return read(b, offset, len);
        } finally {
          seek(oldPos);
        }
      }
      
      @Override
      public synchronized long skip(long n) throws IOException {
        long skipped = 0;
        while (skipped < n) {
          int val = read();
          if (val < 0) {
            break;
          }
          skipped++;
        }
        nextLocation = 0;
        return skipped;
      }
      
      @Override
      public synchronized long getPos() throws IOException {
        nextLocation = 0;
        return currentOffset;
      }
      
      @Override
      public synchronized void seek(long pos) throws IOException {
        if (pos != currentOffset) {
          closeCurrentStream();
          currentOffset = pos;
          openCurrentStream();
        }
        nextLocation = 0;
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        seek(targetPos);
        boolean value = currentStream.seekToNewSource(currentStream.getPos());
        nextLocation = 0;
        return value;
      }
      
      /**
       * position readable again.
       */
      @Override
      public void readFully(long pos, byte[] b, int offset, int length) 
        throws IOException {
        long oldPos = currentOffset;
        seek(pos);
        try {
          while (true) {
            // This loop retries reading until successful. Unrecoverable errors
            // cause exceptions.
            // currentOffset is changed by read().
            try {
              while (length > 0) {
                int n = read(b, offset, length);
                if (n < 0) {
                  throw new IOException("Premature EOF");
                }
                offset += n;
                length -= n;
              }
              nextLocation = 0;
              return;
            } catch (BlockMissingException e) {
              setAlternateLocations(e, currentOffset);
            } catch (ChecksumException e) {
              setAlternateLocations(e, currentOffset);
            }
          }
        } finally {
          seek(oldPos);
        }
      }

      @Override
      public void readFully(long pos, byte[] b) throws IOException {
        readFully(pos, b, 0, b.length);
        nextLocation = 0;
      }

      /**
       * Extract good block from RAID
       * @throws IOException if all alternate locations are exhausted
       */
      private void setAlternateLocations(IOException curexp, long offset) 
        throws IOException {
        while (alternates != null && nextLocation < alternates.length) {
          try {
            int idx = nextLocation++;
            // Start offset of block.
            long corruptOffset =
              (offset / stat.getBlockSize()) * stat.getBlockSize();
            // Make sure we use DFS and not DistributedRaidFileSystem for unRaid.
            Configuration clientConf = new Configuration(conf);
            Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
                                                DistributedFileSystem.class);
            clientConf.set("fs.hdfs.impl", clazz.getName());
            // Disable caching so that a previously cached RaidDfs is not used.
            clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
            Path npath = RaidNode.unRaidCorruptBlock(clientConf, path,
                         alternates[idx].destPath,
                         alternates[idx].createDecoder(),
                         stripeLength, corruptOffset);
            if (npath == null)
              continue;
            try {
              String outdir = conf.get("fs.raid.recoverylogdir");
              if (outdir != null) {
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
                java.util.Date date = new java.util.Date();
                String fname = path.getName() + dateFormat.format(date) +
                (new Random()).nextInt() + ".txt";
                Path outputunraid = new Path(outdir, fname);
                FileSystem fs = outputunraid.getFileSystem(conf);
                FSDataOutputStream dout = fs.create(outputunraid);
                PrintStream ps = new PrintStream(dout);
                ps.println("Recovery attempt log");
                ps.println("Source path : " + path );
                ps.println("Alternate path : " + alternates[idx].destPath);
                ps.println("Stripe lentgh : " + stripeLength);
                ps.println("Corrupt offset : " + corruptOffset);
                String output = (npath==null) ? "UNSUCCESSFUL" : npath.toString();
                ps.println("Output from unRaid : " + output);
                ps.close();
              }
            } catch (Exception exc) {
              LOG.info("Error while creating recovery log: " + exc);
            }

            closeCurrentStream();
            LOG.info("Using block at offset " + corruptOffset + " from " +
              npath);
            currentBlock.path = npath;
            currentBlock.actualFileOffset = 0;  // Single block in file.
            // Dont change currentOffset, in case the user had done a seek?
            openCurrentStream();

            return;
          } catch (Exception e) {
            LOG.info("Error in using alternate path " + path + ". " + e +
                     " Ignoring...");
          }
        }
        throw curexp;
      }

      /**
       * The name of the file system that is immediately below the
       * DistributedRaidFileSystem. This is specified by the
       * configuration parameter called fs.raid.underlyingfs.impl.
       * If this parameter is not specified in the configuration, then
       * the default class DistributedFileSystem is returned.
       * @param conf the configuration object
       * @return the filesystem object immediately below DistributedRaidFileSystem
       * @throws IOException if all alternate locations are exhausted
       */
      private FileSystem getUnderlyingFileSystem(Configuration conf) {
        Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl", DistributedFileSystem.class);
        FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
        return fs;
      }
    }
  
    /**
     * constructor for ext input stream.
     * @param fs the underlying filesystem
     * @param p the path in the underlying file system
     * @param buffersize the size of IO
     * @throws IOException
     */
    public ExtFSDataInputStream(Configuration conf, DistributedRaidFileSystem lfs,
      DecodeInfo[] alternates, Path  p, int stripeLength, int buffersize) throws IOException {
        super(new ExtFsInputStream(conf, lfs, alternates, p, stripeLength, buffersize));
    }
  }
}
