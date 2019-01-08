/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.genesis;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.utils.MetadataStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.apache.hadoop.ozone.genesis.GenesisUtil.CACHE_10MB_TYPE;
import static org.apache.hadoop.ozone.genesis.GenesisUtil.CACHE_1GB_TYPE;
import static org.apache.hadoop.ozone.genesis.GenesisUtil.DEFAULT_TYPE;

/**
 * Measure default metadatastore put performance.
 */
@State(Scope.Thread)
public class BenchMarkMetadataStoreWrites {

  private static final int DATA_LEN = 1024;
  private static final long MAX_KEYS = 1024 * 10;

  private MetadataStore store;
  private byte[] data;

  @Param({DEFAULT_TYPE, CACHE_10MB_TYPE, CACHE_1GB_TYPE})
  private String type;

  @Setup
  public void initialize() throws IOException {
    data = RandomStringUtils.randomAlphanumeric(DATA_LEN)
        .getBytes(Charset.forName("UTF-8"));
    store = GenesisUtil.getMetadataStore(this.type);
  }

  @Benchmark
  public void test() throws IOException {
    long x = org.apache.commons.lang3.RandomUtils.nextLong(0L, MAX_KEYS);
    store.put(Long.toHexString(x).getBytes(Charset.forName("UTF-8")), data);
  }
}
