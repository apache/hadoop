/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * OzoneInputStream is used to read data from Ozone.
 * It uses {@link KeyInputStream} for reading the data.
 */
public class OzoneInputStream extends InputStream {

  private final InputStream inputStream;

  /**
   * Constructs OzoneInputStream with KeyInputStream.
   *
   * @param inputStream
   */
  public OzoneInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputStream.read(b, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    inputStream.close();
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  public InputStream getInputStream() {
    return inputStream;
  }
}
