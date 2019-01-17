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
package org.apache.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import java.io.File;
import java.lang.management.ManagementFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test the JMX interface for the rocksdb metastore implementation.
 */
public class TestRocksDBStoreMBean {
  
  private Configuration conf;
  
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
        OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB);
  }
  

  @Test
  public void testJmxBeans() throws Exception {
    File testDir =
        GenericTestUtils.getTestDir(getClass().getSimpleName() + "-withstat");

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS, "ALL");

    RocksDBStore metadataStore =
        (RocksDBStore) MetadataStoreBuilder.newBuilder().setConf(conf)
            .setCreateIfMissing(true).setDbFile(testDir).build();

    for (int i = 0; i < 10; i++) {
      metadataStore.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    }

    MBeanServer platformMBeanServer =
        ManagementFactory.getPlatformMBeanServer();
    Thread.sleep(2000);

    Object keysWritten = platformMBeanServer
        .getAttribute(metadataStore.getStatMBeanName(), "NUMBER_KEYS_WRITTEN");

    Assert.assertEquals(10L, keysWritten);

    Object dbWriteAverage = platformMBeanServer
        .getAttribute(metadataStore.getStatMBeanName(), "DB_WRITE_AVERAGE");
    Assert.assertTrue((double) dbWriteAverage > 0);

    metadataStore.close();

  }

  @Test()
  public void testDisabledStat() throws Exception {
    File testDir = GenericTestUtils
        .getTestDir(getClass().getSimpleName() + "-withoutstat");

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
        OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF);

    RocksDBStore metadataStore =
        (RocksDBStore) MetadataStoreBuilder.newBuilder().setConf(conf)
            .setCreateIfMissing(true).setDbFile(testDir).build();

    Assert.assertNull(metadataStore.getStatMBeanName());
  }
}