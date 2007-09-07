/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/** Tests per-column bloom filters */
public class TestBloomFilters extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestBloomFilters.class);

  private static final Text CONTENTS = new Text("contents:");

  private static final Text[] rows = {
    new Text("wmjwjzyv"),
    new Text("baietibz"),
    new Text("guhsgxnv"),
    new Text("mhnqycto"),
    new Text("xcyqafgz"),
    new Text("zidoamgb"),
    new Text("tftfirzd"),
    new Text("okapqlrg"),
    new Text("yccwzwsq"),
    new Text("qmonufqu"),
    new Text("wlsctews"),
    new Text("mksdhqri"),
    new Text("wxxllokj"),
    new Text("eviuqpls"),
    new Text("bavotqmj"),
    new Text("yibqzhdl"),
    new Text("csfqmsyr"),
    new Text("guxliyuh"),
    new Text("pzicietj"),
    new Text("qdwgrqwo"),
    new Text("ujfzecmi"),
    new Text("dzeqfvfi"),
    new Text("phoegsij"),
    new Text("bvudfcou"),
    new Text("dowzmciz"),
    new Text("etvhkizp"),
    new Text("rzurqycg"),
    new Text("krqfxuge"),
    new Text("gflcohtd"),
    new Text("fcrcxtps"),
    new Text("qrtovxdq"),
    new Text("aypxwrwi"),
    new Text("dckpyznr"),
    new Text("mdaawnpz"),
    new Text("pakdfvca"),
    new Text("xjglfbez"),
    new Text("xdsecofi"),
    new Text("sjlrfcab"),
    new Text("ebcjawxv"),
    new Text("hkafkjmy"),
    new Text("oimmwaxo"),
    new Text("qcuzrazo"),
    new Text("nqydfkwk"),
    new Text("frybvmlb"),
    new Text("amxmaqws"),
    new Text("gtkovkgx"),
    new Text("vgwxrwss"),
    new Text("xrhzmcep"),
    new Text("tafwziil"),
    new Text("erjmncnv"),
    new Text("heyzqzrn"),
    new Text("sowvyhtu"),
    new Text("heeixgzy"),
    new Text("ktcahcob"),
    new Text("ljhbybgg"),
    new Text("jiqfcksl"),
    new Text("anjdkjhm"),
    new Text("uzcgcuxp"),
    new Text("vzdhjqla"),
    new Text("svhgwwzq"),
    new Text("zhswvhbp"),
    new Text("ueceybwy"),
    new Text("czkqykcw"),
    new Text("ctisayir"),
    new Text("hppbgciu"),
    new Text("nhzgljfk"),
    new Text("vaziqllf"),
    new Text("narvrrij"),
    new Text("kcevbbqi"),
    new Text("qymuaqnp"),
    new Text("pwqpfhsr"),
    new Text("peyeicuk"),
    new Text("kudlwihi"),
    new Text("pkmqejlm"),
    new Text("ylwzjftl"),
    new Text("rhqrlqar"),
    new Text("xmftvzsp"),
    new Text("iaemtihk"),
    new Text("ymsbrqcu"),
    new Text("yfnlcxto"),
    new Text("nluqopqh"),
    new Text("wmrzhtox"),
    new Text("qnffhqbl"),
    new Text("zypqpnbw"),
    new Text("oiokhatd"),
    new Text("mdraddiu"),
    new Text("zqoatltt"),
    new Text("ewhulbtm"),
    new Text("nmswpsdf"),
    new Text("xsjeteqe"),
    new Text("ufubcbma"),
    new Text("phyxvrds"),
    new Text("vhnfldap"),
    new Text("zrrlycmg"),
    new Text("becotcjx"),
    new Text("wvbubokn"),
    new Text("avkgiopr"),
    new Text("mbqqxmrv"),
    new Text("ibplgvuu"),
    new Text("dghvpkgc")
  };

  private static final Text[] testKeys = {
      new Text("abcdefgh"),
      new Text("ijklmnop"),
      new Text("qrstuvwx"),
      new Text("yzabcdef")
  };
  
  /** constructor */
  public TestBloomFilters() {
    super();
    conf.set("hbase.hregion.memcache.flush.size", "100");// flush cache every 100 bytes
    conf.set("hbase.regionserver.maxlogentries", "90"); // and roll log too
  }
  
  /**
   * Test that specifies explicit parameters for the bloom filter
   * @throws IOException
   */
  public void testExplicitParameters() throws IOException {
    HTable table = null;

    // Setup
    
    HTableDescriptor desc = new HTableDescriptor(getName());
    BloomFilterDescriptor bloomFilter =
      new BloomFilterDescriptor(              // if we insert 1000 values
          BloomFilterDescriptor.BloomFilterType.BLOOMFILTER,  // plain old bloom filter
          12499,                              // number of bits
          4                                   // number of hash functions
      );

    desc.addFamily(
        new HColumnDescriptor(CONTENTS,               // Column name
            1,                                        // Max versions
            HColumnDescriptor.CompressionType.NONE,   // no compression
            HColumnDescriptor.DEFAULT_IN_MEMORY,      // not in memory
            HColumnDescriptor.DEFAULT_MAX_VALUE_LENGTH,
            bloomFilter
        )
    );

    // Create the table

    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);

    // Open table

    table = new HTable(conf, desc.getName());

    // Store some values

    for(int i = 0; i < 100; i++) {
      Text row = rows[i];
      String value = row.toString();
      long lockid = table.startUpdate(rows[i]);
      table.put(lockid, CONTENTS, value.getBytes(HConstants.UTF8_ENCODING));
      table.commit(lockid);
    }
    try {
      // Give cache flusher and log roller a chance to run
      // Otherwise we'll never hit the bloom filter, just the memcache
      Thread.sleep(conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000) * 2);
      
    } catch (InterruptedException e) {
      // ignore
    }

    
    for(int i = 0; i < testKeys.length; i++) {
      byte[] value = table.get(testKeys[i], CONTENTS);
      if(value != null && value.length != 0) {
        LOG.info("non existant key: " + testKeys[i] + " returned value: " +
            new String(value, HConstants.UTF8_ENCODING));
      }
    }
  }
  
  /**
   * Test that uses computed for the bloom filter
   * @throws IOException
   */
  public void testComputedParameters() throws IOException {
    HTable table = null;

    // Setup
    
    HTableDescriptor desc = new HTableDescriptor(getName());
      
    BloomFilterDescriptor bloomFilter =
      new BloomFilterDescriptor(
          BloomFilterDescriptor.BloomFilterType.BLOOMFILTER,  // plain old bloom filter
          1000                                  // estimated number of entries
      );
    LOG.info("vector size: " + bloomFilter.vectorSize);

    desc.addFamily(
        new HColumnDescriptor(CONTENTS,               // Column name
            1,                                        // Max versions
            HColumnDescriptor.CompressionType.NONE,   // no compression
            HColumnDescriptor.DEFAULT_IN_MEMORY,      // not in memory
            HColumnDescriptor.DEFAULT_MAX_VALUE_LENGTH,
            bloomFilter
        )
    );

    // Create the table

    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);

    // Open table

    table = new HTable(conf, desc.getName());

    // Store some values

    for(int i = 0; i < 100; i++) {
      Text row = rows[i];
      String value = row.toString();
      long lockid = table.startUpdate(rows[i]);
      table.put(lockid, CONTENTS, value.getBytes(HConstants.UTF8_ENCODING));
      table.commit(lockid);
    }
    try {
      // Give cache flusher and log roller a chance to run
      // Otherwise we'll never hit the bloom filter, just the memcache
      Thread.sleep(conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000) * 2);
      
    } catch (InterruptedException e) {
      // ignore
    }
    
    for(int i = 0; i < testKeys.length; i++) {
      byte[] value = table.get(testKeys[i], CONTENTS);
      if(value != null && value.length != 0) {
        LOG.info("non existant key: " + testKeys[i] + " returned value: " +
            new String(value, HConstants.UTF8_ENCODING));
      }
    }
  }
}
