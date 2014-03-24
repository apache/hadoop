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

package org.apache.hadoop.mapred;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.jboss.netty.channel.DefaultFileRegion;

import com.google.common.annotations.VisibleForTesting;

public class FadvisedFileRegion extends DefaultFileRegion {

  private static final Log LOG = LogFactory.getLog(FadvisedFileRegion.class);

  private final boolean manageOsCache;
  private final int readaheadLength;
  private final ReadaheadPool readaheadPool;
  private final FileDescriptor fd;
  private final String identifier;
  private final long count;
  private final long position;
  private final int shuffleBufferSize;
  private final boolean shuffleTransferToAllowed;
  private final FileChannel fileChannel;
  
  private ReadaheadRequest readaheadRequest;

  public FadvisedFileRegion(RandomAccessFile file, long position, long count,
      boolean manageOsCache, int readaheadLength, ReadaheadPool readaheadPool,
      String identifier, int shuffleBufferSize, 
      boolean shuffleTransferToAllowed) throws IOException {
    super(file.getChannel(), position, count);
    this.manageOsCache = manageOsCache;
    this.readaheadLength = readaheadLength;
    this.readaheadPool = readaheadPool;
    this.fd = file.getFD();
    this.identifier = identifier;
    this.fileChannel = file.getChannel();
    this.count = count;
    this.position = position;
    this.shuffleBufferSize = shuffleBufferSize;
    this.shuffleTransferToAllowed = shuffleTransferToAllowed;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position)
      throws IOException {
    if (manageOsCache && readaheadPool != null) {
      readaheadRequest = readaheadPool.readaheadStream(identifier, fd,
          getPosition() + position, readaheadLength,
          getPosition() + getCount(), readaheadRequest);
    }
    
    if(this.shuffleTransferToAllowed) {
      return super.transferTo(target, position);
    } else {
      return customShuffleTransfer(target, position);
    } 
  }

  /**
   * This method transfers data using local buffer. It transfers data from 
   * a disk to a local buffer in memory, and then it transfers data from the 
   * buffer to the target. This is used only if transferTo is disallowed in
   * the configuration file. super.TransferTo does not perform well on Windows 
   * due to a small IO request generated. customShuffleTransfer can control 
   * the size of the IO requests by changing the size of the intermediate 
   * buffer.
   */
  @VisibleForTesting
  long customShuffleTransfer(WritableByteChannel target, long position)
      throws IOException {
    long actualCount = this.count - position;
    if (actualCount < 0 || position < 0) {
      throw new IllegalArgumentException(
          "position out of range: " + position +
          " (expected: 0 - " + (this.count - 1) + ')');
    }
    if (actualCount == 0) {
      return 0L;
    }
    
    long trans = actualCount;
    int readSize;
    ByteBuffer byteBuffer = ByteBuffer.allocate(this.shuffleBufferSize);
    
    while(trans > 0L &&
        (readSize = fileChannel.read(byteBuffer, this.position+position)) > 0) {
      //adjust counters and buffer limit
      if(readSize < trans) {
        trans -= readSize;
        position += readSize;
        byteBuffer.flip();
      } else {
        //We can read more than we need if the actualCount is not multiple 
        //of the byteBuffer size and file is big enough. In that case we cannot
        //use flip method but we need to set buffer limit manually to trans.
        byteBuffer.limit((int)trans);
        byteBuffer.position(0);
        position += trans; 
        trans = 0;
      }
      
      //write data to the target
      while(byteBuffer.hasRemaining()) {
        target.write(byteBuffer);
      }
      
      byteBuffer.clear();
    }
    
    return actualCount - trans;
  }

  
  @Override
  public void releaseExternalResources() {
    if (readaheadRequest != null) {
      readaheadRequest.cancel();
    }
    super.releaseExternalResources();
  }
  
  /**
   * Call when the transfer completes successfully so we can advise the OS that
   * we don't need the region to be cached anymore.
   */
  public void transferSuccessful() {
    if (manageOsCache && getCount() > 0) {
      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier,
           fd, getPosition(), getCount(),
           NativeIO.POSIX.POSIX_FADV_DONTNEED);
      } catch (Throwable t) {
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }
    }
  }
}
