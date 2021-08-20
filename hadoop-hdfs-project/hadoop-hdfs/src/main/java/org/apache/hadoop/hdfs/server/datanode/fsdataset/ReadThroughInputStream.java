/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is used to implement the read-through caching for PROVIDED
 * replicas.
 */
public abstract class ReadThroughInputStream extends InputStream
    implements Runnable {

  /**
   * Initializes the state of the stream. This should be called before
   * an operations on the stream.
   * @param blockToCache the block to cache.
   * @param conf the configuration
   * @param dataset reference to the FsDataset.
   * @param in the the input stream for the provided replica.
   * @param fileOffset the offset in the PROVIDED storage to which the replica
   *                   being cached refers to.
   * @param seekOffset the seek offset within the replica.
   * @param streamLength the length of the replica.
   * @throws IOException
   */
  public abstract void init(ExtendedBlock blockToCache, Configuration conf,
      FsDatasetSpi dataset, FSDataInputStream in, long fileOffset,
      long seekOffset, long streamLength) throws IOException;
}

