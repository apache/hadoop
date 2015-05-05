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
import java.io.IOException;

/**
 * This holds volume reference count as AutoClosable resource.
 * It increases the reference count by one in the constructor, and decreases
 * the reference count by one in {@link #close()}.
 *
 * <pre>
 *  {@code
 *    try (FsVolumeReference ref = volume.obtainReference()) {
 *      // Do IOs on the volume
 *      volume.createRwb(...);
 *      ...
 *    }
 *  }
 * </pre>
 */
public interface FsVolumeReference extends Closeable {
  /**
   * Decrease the reference count of the volume.
   * @throws IOException it never throws IOException.
   */
  @Override
  void close() throws IOException;

  /**
   * Returns the underlying volume object. Return null if the reference was
   * released.
   */
  FsVolumeSpi getVolume();
}
