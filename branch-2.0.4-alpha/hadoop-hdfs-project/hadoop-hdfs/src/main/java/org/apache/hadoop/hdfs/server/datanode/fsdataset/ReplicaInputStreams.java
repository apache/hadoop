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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.io.IOUtils;

/**
 * Contains the input streams for the data and checksum of a replica.
 */
public class ReplicaInputStreams implements Closeable {
  private final InputStream dataIn;
  private final InputStream checksumIn;

  /** Create an object with a data input stream and a checksum input stream. */
  public ReplicaInputStreams(FileDescriptor dataFd, FileDescriptor checksumFd) {
    this.dataIn = new FileInputStream(dataFd);
    this.checksumIn = new FileInputStream(checksumFd);
  }

  /** @return the data input stream. */
  public InputStream getDataIn() {
    return dataIn;
  }

  /** @return the checksum input stream. */
  public InputStream getChecksumIn() {
    return checksumIn;
  }

  @Override
  public void close() {
    IOUtils.closeStream(dataIn);
    IOUtils.closeStream(checksumIn);
  }
}