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
package org.apache.hadoop.crypto.random;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT;

/**
 * A Random implementation that uses random bytes sourced from the
 * operating system.
 */
@InterfaceAudience.Private
public class OsSecureRandom extends Random implements Closeable, Configurable {
  public static final Log LOG = LogFactory.getLog(OsSecureRandom.class);
  
  private static final long serialVersionUID = 6391500337172057900L;

  private transient Configuration conf;

  private final int RESERVOIR_LENGTH = 8192;

  private String randomDevPath;

  private transient FileInputStream stream;

  private final byte[] reservoir = new byte[RESERVOIR_LENGTH];

  private int pos = reservoir.length;

  private void fillReservoir(int min) {
    if (pos >= reservoir.length - min) {
      try {
        if (stream == null) {
          stream = new FileInputStream(new File(randomDevPath));
        }
        IOUtils.readFully(stream, reservoir, 0, reservoir.length);
      } catch (IOException e) {
        throw new RuntimeException("failed to fill reservoir", e);
      }
      pos = 0;
    }
  }

  public OsSecureRandom() {
  }
  
  @Override
  synchronized public void setConf(Configuration conf) {
    this.conf = conf;
    this.randomDevPath = conf.get(
        HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
        HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);
    close();
  }

  @Override
  synchronized public Configuration getConf() {
    return conf;
  }

  @Override
  synchronized public void nextBytes(byte[] bytes) {
    int off = 0;
    int n = 0;
    while (off < bytes.length) {
      fillReservoir(0);
      n = Math.min(bytes.length - off, reservoir.length - pos);
      System.arraycopy(reservoir, pos, bytes, off, n);
      off += n;
      pos += n;
    }
  }

  @Override
  synchronized protected int next(int nbits) {
    fillReservoir(4);
    int n = 0;
    for (int i = 0; i < 4; i++) {
      n = ((n << 8) | (reservoir[pos++] & 0xff));
    }
    return n & (0xffffffff >> (32 - nbits));
  }

  @Override
  synchronized public void close() {
    if (stream != null) {
      IOUtils.cleanup(LOG, stream);
      stream = null;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    close();
  }
}
