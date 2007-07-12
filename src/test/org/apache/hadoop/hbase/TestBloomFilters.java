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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.hadoop.io.Text;

/** Tests per-column bloom filters */
public class TestBloomFilters extends HBaseClusterTestCase {
  private static final Text CONTENTS = new Text("contents:");

  private HTableDescriptor desc = null;
  private HClient client = null;
  
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
    conf.set("hbase.hregion.maxunflushed", "90"); // flush cache every 100 writes
    conf.set("hbase.regionserver.maxlogentries", "90"); // and roll log too
    Logger.getLogger(HRegion.class).setLevel(Level.DEBUG);
    Logger.getLogger(HStore.class).setLevel(Level.DEBUG);
  }
  
  @Override
  public void setUp() {
    try {
      super.setUp();
      this.client = new HClient(conf);
      this.desc = new HTableDescriptor("test");
      desc.addFamily(
          new HColumnDescriptor(CONTENTS, 1, HColumnDescriptor.CompressionType.NONE,
              false, Integer.MAX_VALUE, 
              new BloomFilterDescriptor(              // if we insert 1000 values
                  BloomFilterDescriptor.BLOOMFILTER,  // plain old bloom filter
                  12499,                              // number of bits
                  4                                   // number of hash functions
              )));                                    // false positive = 0.0000001
      client.createTable(desc);
      client.openTable(desc.getName());

      // Store some values

      for(int i = 0; i < 100; i++) {
        Text row = rows[i];
        String value = row.toString();
        long lockid = client.startUpdate(rows[i]);
        client.put(lockid, CONTENTS, value.getBytes(HConstants.UTF8_ENCODING));
        client.commit(lockid);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /** the test */
  public void testBloomFilters() {
    try {
      // Give cache flusher and log roller a chance to run
      // Otherwise we'll never hit the bloom filter, just the memcache
      Thread.sleep(conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000) * 2);
      
    } catch (InterruptedException e) {
      // ignore
    }
    
    try {
      for(int i = 0; i < testKeys.length; i++) {
        byte[] value = client.get(testKeys[i], CONTENTS);
        if(value != null && value.length != 0) {
          System.err.println("non existant key: " + testKeys[i] +
              " returned value: " + new String(value, HConstants.UTF8_ENCODING));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
