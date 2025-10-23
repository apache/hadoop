/*
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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.model.object.GetObjectV2Output;
import org.apache.hadoop.fs.tosfs.object.exceptions.ChecksumMismatchException;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class GetObjectOutput {
  private final GetObjectV2Output output;
  private final byte[] checksum;

  public GetObjectOutput(GetObjectV2Output output, byte[] checksum) {
    Preconditions.checkNotNull(checksum, "Checksum should not be null.");
    this.output = output;
    this.checksum = checksum;
  }

  public GetObjectV2Output output() {
    return output;
  }

  public byte[] checksum() {
    return checksum;
  }

  public InputStream verifiedContent(byte[] expectedChecksum) throws IOException {
    if (!Arrays.equals(expectedChecksum, checksum)) {
      CommonUtils.runQuietly(this::forceClose);
      throw new ChecksumMismatchException(expectedChecksum, checksum);
    }

    return output.getContent();
  }

  public void forceClose() throws IOException {
    output.forceClose();
  }
}
