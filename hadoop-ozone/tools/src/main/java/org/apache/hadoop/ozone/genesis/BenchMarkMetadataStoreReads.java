package org.apache.hadoop.ozone.genesis;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.utils.MetadataStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.apache.hadoop.ozone.genesis.GenesisUtil.CACHE_10MB_TYPE;
import static org.apache.hadoop.ozone.genesis.GenesisUtil.CACHE_1GB_TYPE;
import static org.apache.hadoop.ozone.genesis.GenesisUtil.CLOSED_TYPE;
import static org.apache.hadoop.ozone.genesis.GenesisUtil.DEFAULT_TYPE;

@State(Scope.Thread)
public class BenchMarkMetadataStoreReads {

  private static final int DATA_LEN = 1024;
  private static final long maxKeys = 1024 * 10;

  private MetadataStore store;

  @Param({DEFAULT_TYPE, CACHE_10MB_TYPE, CACHE_1GB_TYPE, CLOSED_TYPE})
  private String type;

  @Setup
  public void initialize() throws IOException {
    store = GenesisUtil.getMetadataStore(this.type);
    byte[] data = RandomStringUtils.randomAlphanumeric(DATA_LEN)
        .getBytes(Charset.forName("UTF-8"));
    for (int x = 0; x < maxKeys; x++) {
      store.put(Long.toHexString(x).getBytes(Charset.forName("UTF-8")), data);
    }
    if (type.compareTo(CLOSED_TYPE) == 0) {
      store.compactDB();
    }
  }

  @Benchmark
  public void test(Blackhole bh) throws IOException {
    long x = org.apache.commons.lang3.RandomUtils.nextLong(0L, maxKeys);
    bh.consume(
        store.get(Long.toHexString(x).getBytes(Charset.forName("UTF-8"))));
  }
}
