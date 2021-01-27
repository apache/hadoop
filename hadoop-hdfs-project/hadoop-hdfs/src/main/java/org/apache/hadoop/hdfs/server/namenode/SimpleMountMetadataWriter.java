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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.mountmanager.MountMetadataWriter;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.TreePath;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of {@link MountMetadataWriter} that just assigns
 * increasing ids to the Treepaths accepted.
 */
public class SimpleMountMetadataWriter implements MountMetadataWriter,
    Closeable {

  /** Latest assigned inode id. */
  private AtomicLong inodeId = new AtomicLong(0);

  @Override
  public void accept(TreePath e) throws IOException {
    assert e.getParentId() < inodeId.get();
    // allocate ID
    long id = inodeId.getAndIncrement();
    e.accept(id);
  }

  @Override
  public void close() throws IOException {
    // NO OP
  }
}
