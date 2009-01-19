/**
 * Copyright (c) 2005, European Commission project OneLab under contract 034819
 * (http://www.one-lab.org)
 * 
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following 
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior 
 *    written permission.
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
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
package org.onelab.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Hash;
import org.onelab.filter.*;

/**
 * Test class.
 * 
 * contract <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @version 1.0 - 8 Feb. 07
 */
public class TestFilter extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestFilter.class);
  
  /** Test a BloomFilter
   * @throws UnsupportedEncodingException
   * @throws IOException
   */
  public void testBloomFilter() throws UnsupportedEncodingException,
  IOException {
    final StringKey[] inserted = {
        new StringKey("wmjwjzyv"),
        new StringKey("baietibz"),
        new StringKey("guhsgxnv"),
        new StringKey("mhnqycto"),
        new StringKey("xcyqafgz"),
        new StringKey("zidoamgb"),
        new StringKey("tftfirzd"),
        new StringKey("okapqlrg"),
        new StringKey("yccwzwsq"),
        new StringKey("qmonufqu"),
        new StringKey("wlsctews"),
        new StringKey("mksdhqri"),
        new StringKey("wxxllokj"),
        new StringKey("eviuqpls"),
        new StringKey("bavotqmj"),
        new StringKey("yibqzhdl"),
        new StringKey("csfqmsyr"),
        new StringKey("guxliyuh"),
        new StringKey("pzicietj"),
        new StringKey("qdwgrqwo"),
        new StringKey("ujfzecmi"),
        new StringKey("dzeqfvfi"),
        new StringKey("phoegsij"),
        new StringKey("bvudfcou"),
        new StringKey("dowzmciz"),
        new StringKey("etvhkizp"),
        new StringKey("rzurqycg"),
        new StringKey("krqfxuge"),
        new StringKey("gflcohtd"),
        new StringKey("fcrcxtps"),
        new StringKey("qrtovxdq"),
        new StringKey("aypxwrwi"),
        new StringKey("dckpyznr"),
        new StringKey("mdaawnpz"),
        new StringKey("pakdfvca"),
        new StringKey("xjglfbez"),
        new StringKey("xdsecofi"),
        new StringKey("sjlrfcab"),
        new StringKey("ebcjawxv"),
        new StringKey("hkafkjmy"),
        new StringKey("oimmwaxo"),
        new StringKey("qcuzrazo"),
        new StringKey("nqydfkwk"),
        new StringKey("frybvmlb"),
        new StringKey("amxmaqws"),
        new StringKey("gtkovkgx"),
        new StringKey("vgwxrwss"),
        new StringKey("xrhzmcep"),
        new StringKey("tafwziil"),
        new StringKey("erjmncnv"),
        new StringKey("heyzqzrn"),
        new StringKey("sowvyhtu"),
        new StringKey("heeixgzy"),
        new StringKey("ktcahcob"),
        new StringKey("ljhbybgg"),
        new StringKey("jiqfcksl"),
        new StringKey("anjdkjhm"),
        new StringKey("uzcgcuxp"),
        new StringKey("vzdhjqla"),
        new StringKey("svhgwwzq"),
        new StringKey("zhswvhbp"),
        new StringKey("ueceybwy"),
        new StringKey("czkqykcw"),
        new StringKey("ctisayir"),
        new StringKey("hppbgciu"),
        new StringKey("nhzgljfk"),
        new StringKey("vaziqllf"),
        new StringKey("narvrrij"),
        new StringKey("kcevbbqi"),
        new StringKey("qymuaqnp"),
        new StringKey("pwqpfhsr"),
        new StringKey("peyeicuk"),
        new StringKey("kudlwihi"),
        new StringKey("pkmqejlm"),
        new StringKey("ylwzjftl"),
        new StringKey("rhqrlqar"),
        new StringKey("xmftvzsp"),
        new StringKey("iaemtihk"),
        new StringKey("ymsbrqcu"),
        new StringKey("yfnlcxto"),
        new StringKey("nluqopqh"),
        new StringKey("wmrzhtox"),
        new StringKey("qnffhqbl"),
        new StringKey("zypqpnbw"),
        new StringKey("oiokhatd"),
        new StringKey("mdraddiu"),
        new StringKey("zqoatltt"),
        new StringKey("ewhulbtm"),
        new StringKey("nmswpsdf"),
        new StringKey("xsjeteqe"),
        new StringKey("ufubcbma"),
        new StringKey("phyxvrds"),
        new StringKey("vhnfldap"),
        new StringKey("zrrlycmg"),
        new StringKey("becotcjx"),
        new StringKey("wvbubokn"),
        new StringKey("avkgiopr"),
        new StringKey("mbqqxmrv"),
        new StringKey("ibplgvuu"),
        new StringKey("dghvpkgc")
    };

    final StringKey[] notInserted = {
        new StringKey("abcdefgh"),
        new StringKey("ijklmnop"),
        new StringKey("qrstuvwx"),
        new StringKey("yzabcdef")
    };

    /* 
     * Bloom filters are very sensitive to the number of elements inserted into
     * them.
     * 
     * If m denotes the number of bits in the Bloom filter (vectorSize),
     * n denotes the number of elements inserted into the Bloom filter and
     * k represents the number of hash functions used (nbHash), then
     * according to Broder and Mitzenmacher,
     * 
     * ( http://www.eecs.harvard.edu/~michaelm/NEWWORK/postscripts/BloomFilterSurvey.pdf )
     * 
     * the probability of false positives is minimized when k is
     * approximately ln(2) * m/n.
     * 
     * If we fix the number of hash functions and know the number of entries,
     * then the optimal vector size m = (k * n) / ln(2)
     */
    final int DEFAULT_NUMBER_OF_HASH_FUNCTIONS = 4;
    BloomFilter bf = new BloomFilter(
        (int) Math.ceil(
            (DEFAULT_NUMBER_OF_HASH_FUNCTIONS * (1.0 * inserted.length)) /
            Math.log(2.0)),
            DEFAULT_NUMBER_OF_HASH_FUNCTIONS,
            Hash.JENKINS_HASH
    );
    
    for (int i = 0; i < inserted.length; i++) {
      bf.add(inserted[i]);
    }
    
    // Verify that there are no false negatives and few (if any) false positives
    
    checkFalsePositivesNegatives(bf, inserted, notInserted);

    // Test serialization/deserialization
    
    LOG.info("Checking serialization/deserialization");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    bf.write(out);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);
    bf = new BloomFilter();
    bf.readFields(in);
   
    // Verify that there are no false negatives and few (if any) false positives
    
    checkFalsePositivesNegatives(bf, inserted, notInserted);
  }
  
  private void checkFalsePositivesNegatives(BloomFilter bf,
      StringKey[] inserted, StringKey[] notInserted) {
    // Test membership for values we inserted. Should not get false negatives
    
    LOG.info("Checking for false negatives");
    for (int i = 0; i < inserted.length; i++) {
      if (!bf.membershipTest(inserted[i])) {
        LOG.error("false negative for: " + inserted[i]);
        fail();
      }
    }
    
    // Test membership for values we did not insert. It is possible to get
    // false positives
    
    LOG.info("Checking for false positives");
    for (int i = 0; i < notInserted.length; i++) {
      if(bf.membershipTest(notInserted[i])) {
        LOG.error("false positive for: " + notInserted[i]);
        fail();
      }
    }
    LOG.info("Success!");
  }
  
  /** Test a CountingBloomFilter
   * @throws UnsupportedEncodingException
   */
  public void testCountingBloomFilter() throws UnsupportedEncodingException {
    Filter bf = new CountingBloomFilter(8, 2, Hash.JENKINS_HASH);
    Key key = new StringKey("toto");
    Key k2 = new StringKey("lulu");
    Key k3 = new StringKey("mama");
    bf.add(key);
    bf.add(k2);
    bf.add(k3);
    assertTrue(bf.membershipTest(key));
    assertFalse(bf.membershipTest(k2));
    assertFalse(bf.membershipTest(new StringKey("xyzzy")));
    assertFalse(bf.membershipTest(new StringKey("abcd")));

    // delete 'key', and check that it is no longer a member
    ((CountingBloomFilter)bf).delete(key);
    assertFalse(bf.membershipTest(key));
    
    // OR 'key' back into the filter
    Filter bf2 = new CountingBloomFilter(8, 2, Hash.JENKINS_HASH);
    bf2.add(key);
    bf.or(bf2);
    assertTrue(bf.membershipTest(key));
    assertTrue(bf.membershipTest(k2));
    assertFalse(bf.membershipTest(new StringKey("xyzzy")));
    assertFalse(bf.membershipTest(new StringKey("abcd")));
    
    // to test for overflows, add 'key' enough times to overflow a 4bit bucket,
    // while asserting that it stays a member
    for(int i = 0; i < 16; i++){
      bf.add(key);
      assertTrue(bf.membershipTest(key));
    }
    // test approximateCount
    CountingBloomFilter bf3 = new CountingBloomFilter(4, 2, Hash.JENKINS_HASH);
    // test the exact range
    for (int i = 0; i < 8; i++) {
      bf3.add(key);
      bf3.add(k2);
      assertEquals(bf3.approximateCount(key), i + 1);
      assertEquals(bf3.approximateCount(k2), i + 1);
    }
    // test gently degraded counting in high-fill, high error rate filter
    for (int i = 8; i < 15; i++) {
      bf3.add(key);
      assertTrue(bf3.approximateCount(key) >= (i + 1));
      assertEquals(bf3.approximateCount(k2), 8);
      assertEquals(bf3.approximateCount(k3), 0);
    }
  }
  
  /** Test a DynamicBloomFilter
   * @throws UnsupportedEncodingException
   */
  public void testDynamicBloomFilter() throws UnsupportedEncodingException {
    Filter bf = new DynamicBloomFilter(8, 2, Hash.JENKINS_HASH, 2);
    Key key = new StringKey("toto");
    Key k2 = new StringKey("lulu");
    Key k3 = new StringKey("mama");
    bf.add(key);
    bf.add(k2);
    bf.add(k3);
    assertTrue(bf.membershipTest(key));
    assertTrue(bf.membershipTest(new StringKey("graknyl")));
    assertFalse(bf.membershipTest(new StringKey("xyzzy")));
    assertFalse(bf.membershipTest(new StringKey("abcd")));
  }
}//end class
