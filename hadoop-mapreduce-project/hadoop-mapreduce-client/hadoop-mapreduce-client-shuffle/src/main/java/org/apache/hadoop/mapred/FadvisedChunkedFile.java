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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.jboss.netty.handler.stream.ChunkedFile;

public class FadvisedChunkedFile extends ChunkedFile {

  private static final Log LOG = LogFactory.getLog(FadvisedChunkedFile.class);

  private final boolean manageOsCache;
  private final int readaheadLength;
  private final ReadaheadPool readaheadPool;
  private final FileDescriptor fd;
  private final String identifier;

  private ReadaheadRequest readaheadRequest;

  public FadvisedChunkedFile(RandomAccessFile file, long position, long count,
      int chunkSize, boolean manageOsCache, int readaheadLength,
      ReadaheadPool readaheadPool, String identifier) throws IOException {
    super(file, position, count, chunkSize);
    this.manageOsCache = manageOsCache;
    this.readaheadLength = readaheadLength;
    this.readaheadPool = readaheadPool;
    this.fd = file.getFD();
    this.identifier = identifier;
  }

  @Override
  public Object nextChunk() throws Exception {
    if (manageOsCache && readaheadPool != null) {
      readaheadRequest = readaheadPool
          .readaheadStream(identifier, fd, getCurrentOffset(), readaheadLength,
              getEndOffset(), readaheadRequest);
    }
    return super.nextChunk();
  }

  @Override
  public void close() throws Exception {
    if (readaheadRequest != null) {
      readaheadRequest.cancel();
    }
    if (manageOsCache && getEndOffset() - getStartOffset() > 0) {
      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier,
            fd,
            getStartOffset(), getEndOffset() - getStartOffset(),
            NativeIO.POSIX.POSIX_FADV_DONTNEED);
      } catch (Throwable t) {
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }
    }
    super.close();
  }
}
