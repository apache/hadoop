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

package org.apache.hadoop.streaming;

import java.io.*;

/**
 * A simple Java app that will consume all input from stdin, wait a few seconds
 * and echoing it to stdout.
 */
public class DelayEchoApp {

  public DelayEchoApp() {
  }

  public void go(int seconds) throws IOException, InterruptedException {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;

    // Consume all input (to make sure streaming will still count this
    // task as failed even if all input was consumed).
    while ((line = in.readLine()) != null) {
      Thread.sleep(seconds * 1000L);
      System.out.println(line);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    int seconds = 5;
    if (args.length >= 1) {
      try {
        seconds = Integer.parseInt(args[0]);
      } catch (NumberFormatException e) {
        // just use default 5.
      }
    }

    DelayEchoApp app = new DelayEchoApp();
    app.go(seconds);
  }
}
