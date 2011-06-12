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

package org.apache.hadoop.sqoop.util;

import java.io.InputStream;

/**
 * An interface describing a factory class for a Thread class that handles
 * input from some sort of stream.
 *
 * When the stream is closed, the thread should terminate.
 *
 */
public interface StreamHandlerFactory {
  
  /**
   * Create and run a thread to handle input from the provided InputStream.
   * When processStream returns, the thread should be running; it should
   * continue to run until the InputStream is exhausted.
   */
  void processStream(InputStream is);

  /**
   * Wait until the stream has been processed.
   * @return a status code indicating success or failure. 0 is typical for success.
   */
  int join() throws InterruptedException;
}

