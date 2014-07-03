/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.swift.exceptions.SwiftConnectionClosedException;

import java.io.EOFException;
import java.io.IOException;

/**
 * Add stricter compliance with the evolving FS specifications
 */
public class StrictBufferedFSInputStream extends BufferedFSInputStream {

  public StrictBufferedFSInputStream(FSInputStream in,
                                     int size) {
    super(in, size);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    if (in == null) {
      throw new SwiftConnectionClosedException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    super.seek(pos);
  }
}
