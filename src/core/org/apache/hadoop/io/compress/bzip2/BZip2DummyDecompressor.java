/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.io.compress.bzip2;

import java.io.IOException;

import org.apache.hadoop.io.compress.Decompressor;

/**
 * This is a dummy decompressor for BZip2.
 */
public class BZip2DummyDecompressor implements Decompressor {

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void end() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean finished() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean needsDictionary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean needsInput() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRemaining() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reset() {
    // do nothing
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

}
