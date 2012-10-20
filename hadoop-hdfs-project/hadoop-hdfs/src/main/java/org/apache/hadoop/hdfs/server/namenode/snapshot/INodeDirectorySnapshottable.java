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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;

/** Directories where taking snapshots is allowed. */
@InterfaceAudience.Private
public class INodeDirectorySnapshottable extends INodeDirectoryWithQuota {
  static public INodeDirectorySnapshottable newInstance(final INodeDirectory dir) {
    long nsq = -1L;
    long dsq = -1L;

    if (dir instanceof INodeDirectoryWithQuota) {
      final INodeDirectoryWithQuota q = (INodeDirectoryWithQuota)dir;
      nsq = q.getNsQuota();
      dsq = q.getDsQuota();
    }
    return new INodeDirectorySnapshottable(nsq, dsq, dir);
  }

  private INodeDirectorySnapshottable(long nsQuota, long dsQuota,
      INodeDirectory dir) {
    super(nsQuota, dsQuota, dir);
  }

  @Override
  public boolean isSnapshottable() {
    return true;
  }
}
