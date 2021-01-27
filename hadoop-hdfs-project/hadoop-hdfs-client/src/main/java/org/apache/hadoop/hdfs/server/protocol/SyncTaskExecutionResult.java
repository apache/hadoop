/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;

import java.nio.ByteBuffer;

/**
 * Result of a SyncTask.
 */
public class SyncTaskExecutionResult {

  /** result is the opaque byte stream result of a task. e.g. PartHandle */
  private ByteBuffer result;
  private Long numberOfBytes;

  public SyncTaskExecutionResult(ByteBuffer result, Long numberOfBytes) {
    this.result = copyResult(result);
    this.numberOfBytes = numberOfBytes;
  }

  public static SyncTaskExecutionResult emptyResult() {
    return new SyncTaskExecutionResult(ByteBuffer.wrap(new byte[0]), 0L);
  }

  public ByteBuffer getResult() {
    return result;
  }

  public Long getNumberOfBytes() {
    return numberOfBytes;
  }

  private ByteBuffer copyResult(ByteBuffer executionResult) {
    ByteBuffer resultCopy = ByteBuffer.allocate(executionResult.capacity());
    executionResult.rewind();
    resultCopy.put(executionResult);
    executionResult.rewind();
    resultCopy.flip();
    return resultCopy;
  }
}
