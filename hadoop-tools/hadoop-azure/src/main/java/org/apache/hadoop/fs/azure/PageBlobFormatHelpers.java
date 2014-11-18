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

package org.apache.hadoop.fs.azure;

import java.nio.ByteBuffer;

import com.microsoft.windowsazure.storage.blob.BlobRequestOptions;

/**
 * Constants and helper methods for ASV's custom data format in page blobs.
 */
final class PageBlobFormatHelpers {
  public static final short PAGE_SIZE = 512;
  public static final short PAGE_HEADER_SIZE = 2;
  public static final short PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;

  // Hide constructor for utility class.
  private PageBlobFormatHelpers() {
    
  }
  
  /**
   * Stores the given short as a two-byte array.
   */
  public static byte[] fromShort(short s) {
    return ByteBuffer.allocate(2).putShort(s).array();
  }

  /**
   * Retrieves a short from the given two bytes.
   */
  public static short toShort(byte firstByte, byte secondByte) {
    return ByteBuffer.wrap(new byte[] { firstByte, secondByte })
        .getShort();
  }

  public static BlobRequestOptions withMD5Checking() {
    BlobRequestOptions options = new BlobRequestOptions();
    options.setUseTransactionalContentMD5(true);
    return options;
  }
}
