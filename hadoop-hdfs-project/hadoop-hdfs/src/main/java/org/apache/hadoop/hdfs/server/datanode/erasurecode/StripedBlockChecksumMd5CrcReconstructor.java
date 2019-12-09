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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.io.IOException;
import java.security.MessageDigest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;

/**
 * Computes running MD5-of-CRC over reconstructed chunk CRCs.
 */
@InterfaceAudience.Private
public class StripedBlockChecksumMd5CrcReconstructor
    extends StripedBlockChecksumReconstructor {
  private MD5Hash md5;
  private MessageDigest digester;

  public StripedBlockChecksumMd5CrcReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo,
      DataOutputBuffer checksumWriter,
      long requestedBlockLength) throws IOException {
    super(worker, stripedReconInfo, checksumWriter, requestedBlockLength);
  }

  @Override
  public Object getDigestObject() {
    return md5;
  }

  @Override
  void prepareDigester() throws IOException {
    digester = MD5Hash.getDigester();
  }

  @Override
  void updateDigester(byte[] checksumBytes, int dataBytesPerChecksum)
      throws IOException {
    if (digester == null) {
      throw new IOException(String.format(
          "Called updatedDigester with checksumBytes.length=%d, "
          + "dataBytesPerChecksum=%d but digester is null",
          checksumBytes.length, dataBytesPerChecksum));
    }
    digester.update(checksumBytes, 0, checksumBytes.length);
  }

  @Override
  void commitDigest() throws IOException {
    if (digester == null) {
      throw new IOException("Called commitDigest() but digester is null");
    }
    byte[] digest = digester.digest();
    md5 = new MD5Hash(digest);
    md5.write(getChecksumWriter());
  }
}
