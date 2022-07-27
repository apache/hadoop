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

package org.apache.hadoop.ipc.netty.server;

import org.apache.hadoop.ipc.Server;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

// netty event loops must be created for the bootstrap before binding.
// this custom thread factory provides a way to (re)name the threads and
// include the bound port including after-the-fact for ephemeral port or
// port ranged binds.
public class NettyThreadFactory extends ThreadGroup
    implements ThreadFactory, Closeable {
  private final Server server;
  private final AtomicInteger count = new AtomicInteger();
  private final String format;
  private int port;

  public NettyThreadFactory(Server server, String prefix, int port) {
    super(server.getClass().getSimpleName() + " " + prefix + "s");
    this.server = server;
    this.format = prefix + " #%d for port %s";
    this.port = port;
    setDaemon(true);
  }

  @Override
  public synchronized Thread newThread(Runnable r) {
    return new Thread(r) {
      @Override
      public void run() {
        Server.SERVER.set(server);
        setName(String.format(format, count.incrementAndGet(),
            (port != 0 ? port : "%s")));
        super.run();
      }
    };
  }

  public synchronized void updatePort(int listenPort) {
    if (port == 0) {
      port = listenPort;
      Thread[] threads = new Thread[count.get()];
      int actual = enumerate(threads);
      for (int i = 0; i < actual; i++) {
        threads[i].setName(String.format(threads[i].getName(), port));
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!isDestroyed()) {
      destroy();
    }
  }
}
