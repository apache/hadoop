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

package org.apache.hadoop.mapreduce.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * get/set, store/load security keys
 * key's value - byte[]
 * store/load from DataInput/DataOuptut
 * List of currently store keys:
 *  jobToken for secure shuffle HTTP Get
 *
 */
public class JobTokens implements Writable {
  /**
   * file name used on HDFS for generated keys
   */
  public static final String JOB_TOKEN_FILENAME = "jobTokens";

  private byte [] shuffleJobToken = null; // jobtoken for shuffle (map output)

  
  /**
   * returns the key value for the alias
   * @return key for this alias
   */
  public byte[] getShuffleJobToken() {
    return shuffleJobToken;
  }
  
  /**
   * sets the jobToken
   * @param key
   */
  public void setShuffleJobToken(byte[] key) {
    shuffleJobToken = key;
  }
  
  /**
   * stores all the keys to DataOutput
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeCompressedByteArray(out, shuffleJobToken);
  }
  
  /**
   * loads all the keys
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    shuffleJobToken = WritableUtils.readCompressedByteArray(in);
  }
}
