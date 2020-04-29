/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test of the URL stream handler factory.
 */
public class TestUrlStreamHandlerFactory {

  private static final int RUNS = 20;
  private static final int THREADS = 10;
  private static final int TASKS = 200;

  @Rule
  public Timeout globalTimeout = new Timeout(30000);

  @Test
  public void testConcurrency() throws Exception {
    for (int i = 0; i < RUNS; i++) {
      singleRun();
    }
  }

  private void singleRun() throws Exception {
    final FsUrlStreamHandlerFactory factory = new FsUrlStreamHandlerFactory();
    final Random random = new Random();
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    ArrayList<Future<?>> futures = new ArrayList<Future<?>>(TASKS);

    for (int i = 0; i < TASKS ; i++) {
      final int aux = i;
      futures.add(executor.submit(new Runnable() {
        @Override
        public void run() {
          int rand = aux + random.nextInt(3);
          factory.createURLStreamHandler(String.valueOf(rand));
        }
      }));
    }

    executor.shutdown();

    // check for exceptions
    for (Future future : futures) {
      if (!future.isDone()) {
        break; // timed out
      }
      future.get();
    }
  }

  @Test
  public void testFsUrlStreamHandlerFactory() throws IOException {
    File myFile = new File(GenericTestUtils.getTestDir(), "foo bar.txt");
    myFile.createNewFile();

    // Create URL directly from File (JRE builds it).
    URL myUrl = myFile.toURI().toURL();

    // Succeeds.
    myUrl.openStream().close();

    // Replace handling of file: scheme with FsUrlStreamHandler.
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

    URL myUrl2 = myFile.toURI().toURL();

    myUrl2.openStream();
  }
}
