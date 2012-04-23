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
package org.apache.hadoop.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;


  /**
 * The API shared between local and remote block readers.
   */
public interface BlockReader extends Closeable {

  public int read(byte buf[], int off, int len) throws IOException;

  public int readAll(byte[] buf, int offset, int len) throws IOException;

  public long skip(long n) throws IOException;

  /**
   * Take the socket used to talk to the DN.
   */
  Socket takeSocket();

  /**
   * Whether the BlockReader has reached the end of its input stream
   * and successfully sent a status code back to the datanode.
   */
  boolean hasSentStatusCode();
}
