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
package org.apache.hadoop.ozone.web.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.hadoop.io.IOUtils;

import java.io.Closeable;

/**
 * A {@link ChannelFutureListener} that closes {@link Closeable} resources.
 */
final class CloseableCleanupListener implements ChannelFutureListener {

  private final Closeable[] closeables;

  /**
   * Creates a new CloseableCleanupListener.
   *
   * @param closeables any number of closeable resources
   */
  CloseableCleanupListener(Closeable... closeables) {
    this.closeables = closeables;
  }

  @Override
  public void operationComplete(ChannelFuture future) {
    IOUtils.cleanupWithLogger(null, closeables);
  }
}
