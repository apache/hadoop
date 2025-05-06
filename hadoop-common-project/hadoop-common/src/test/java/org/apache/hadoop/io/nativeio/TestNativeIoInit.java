/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.nativeio;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Separate class to ensure forked Tests load the static blocks again.
 */
public class TestNativeIoInit {

  /**
   * Refer HADOOP-14451
   * Scenario:
   * 1. One thread calls a static method of NativeIO, which loads static block
   * of NativeIo.
   * 2. Second thread calls a static method of NativeIo.POSIX, which loads a
   * static block of NativeIO.POSIX class
   * <p>
   * Expected: Loading these two static blocks separately should not result in
   * deadlock.
   */
  @Test
  @Timeout(value = 10)
  public void testDeadlockLinux() throws Exception {
    Thread one = new Thread() {
      @Override
      public void run() {
        NativeIO.isAvailable();
      }
    };
    Thread two = new Thread() {
      @Override
      public void run() {
        NativeIO.POSIX.isAvailable();
      }
    };
    two.start();
    one.start();
    one.join();
    two.join();
  }

  @Test
  @Timeout(value = 10)
  public void testDeadlockWindows() throws Exception {
    assumeTrue(Path.WINDOWS, "Expected windows");
    Thread one = new Thread() {
      @Override
      public void run() {
        NativeIO.isAvailable();
      }
    };
    Thread two = new Thread() {
      @Override
      public void run() {
        try {
          NativeIO.Windows.extendWorkingSetSize(100);
        } catch (IOException e) {
          //igored
        }
      }
    };
    two.start();
    one.start();
    one.join();
    two.join();
  }
}
