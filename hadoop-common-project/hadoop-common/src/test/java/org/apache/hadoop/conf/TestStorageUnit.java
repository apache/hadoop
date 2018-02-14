/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.conf;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests that Storage Units work as expected.
 */
public class TestStorageUnit {
  final static double KB = 1024.0;
  final static double MB = KB * 1024.0;
  final static double GB = MB * 1024.0;
  final static double TB = GB * 1024.0;
  final static double PB = TB * 1024.0;
  final static double EB = PB * 1024.0;

  @Test
  public void testByteToKiloBytes() {
    Map<Double, Double> results = new HashMap<>();
    results.put(1024.0, 1.0);
    results.put(2048.0, 2.0);
    results.put(-1024.0, -1.0);
    results.put(34565.0, 33.7549);
    results.put(223344332.0, 218109.6992);
    results.put(1234983.0, 1206.0381);
    results.put(1234332.0, 1205.4023);
    results.put(0.0, 0.0);

    for (Map.Entry<Double, Double> entry : results.entrySet()) {
      assertThat(StorageUnit.BYTES.toKBs(entry.getKey()), is(entry.getValue()));
    }
  }

  @Test
  public void testBytesToMegaBytes() {
    Map<Double, Double> results = new HashMap<>();
    results.put(1048576.0, 1.0);
    results.put(24117248.0, 23.0);
    results.put(459920023.0, 438.6139);
    results.put(234443233.0, 223.5825);
    results.put(-35651584.0, -34.0);
    results.put(0.0, 0.0);
    for (Map.Entry<Double, Double> entry : results.entrySet()) {
      assertThat(StorageUnit.BYTES.toMBs(entry.getKey()), is(entry.getValue()));
    }
  }

  @Test
  public void testBytesToGigaBytes() {
    Map<Double, Double> results = new HashMap<>();
    results.put(1073741824.0, 1.0);
    results.put(24696061952.0, 23.0);
    results.put(459920023.0, 0.4283);
    results.put(234443233.0, 0.2183);
    results.put(-36507222016.0, -34.0);
    results.put(0.0, 0.0);
    for (Map.Entry<Double, Double> entry : results.entrySet()) {
      assertThat(StorageUnit.BYTES.toGBs(entry.getKey()), is(entry.getValue()));
    }
  }

  @Test
  public void testBytesToTerraBytes() {
    Map<Double, Double> results = new HashMap<>();
    results.put(1.09951E+12, 1.0);
    results.put(2.52888E+13, 23.0);
    results.put(459920023.0, 0.0004);
    results.put(234443233.0, 0.0002);
    results.put(-3.73834E+13, -34.0);
    results.put(0.0, 0.0);
    for (Map.Entry<Double, Double> entry : results.entrySet()) {
      assertThat(StorageUnit.BYTES.toTBs(entry.getKey()), is(entry.getValue()));
    }
  }

  @Test
  public void testBytesToPetaBytes() {
    Map<Double, Double> results = new HashMap<>();
    results.put(1.1259E+15, 1.0);
    results.put(2.58957E+16, 23.0);
    results.put(4.70958E+11, 0.0004);
    results.put(234443233.0, 0.0000); // Out of precision window.
    results.put(-3.82806E+16, -34.0);
    results.put(0.0, 0.0);
    for (Map.Entry<Double, Double> entry : results.entrySet()) {
      assertThat(StorageUnit.BYTES.toPBs(entry.getKey()), is(entry.getValue()));
    }
  }

  @Test
  public void testBytesToExaBytes() {
    Map<Double, Double> results = new HashMap<>();
    results.put(1.15292E+18, 1.0);
    results.put(2.65172E+19, 23.0);
    results.put(4.82261E+14, 0.0004);
    results.put(234443233.0, 0.0000); // Out of precision window.
    results.put(-3.91993E+19, -34.0);
    results.put(0.0, 0.0);
    for (Map.Entry<Double, Double> entry : results.entrySet()) {
      assertThat(StorageUnit.BYTES.toEBs(entry.getKey()), is(entry.getValue()));
    }
  }

  @Test
  public void testByteConversions() {
    assertThat(StorageUnit.BYTES.getShortName(), is("b"));
    assertThat(StorageUnit.BYTES.getSuffixChar(), is("b"));

    assertThat(StorageUnit.BYTES.getLongName(), is("bytes"));
    assertThat(StorageUnit.BYTES.toString(), is("bytes"));
    assertThat(StorageUnit.BYTES.toBytes(1), is(1.0));
    assertThat(StorageUnit.BYTES.toBytes(1024),
        is(StorageUnit.BYTES.getDefault(1024)));
    assertThat(StorageUnit.BYTES.fromBytes(10), is(10.0));
  }

  @Test
  public void testKBConversions() {
    assertThat(StorageUnit.KB.getShortName(), is("kb"));
    assertThat(StorageUnit.KB.getSuffixChar(), is("k"));
    assertThat(StorageUnit.KB.getLongName(), is("kilobytes"));
    assertThat(StorageUnit.KB.toString(), is("kilobytes"));
    assertThat(StorageUnit.KB.toKBs(1024),
        is(StorageUnit.KB.getDefault(1024)));


    assertThat(StorageUnit.KB.toBytes(1), is(KB));
    assertThat(StorageUnit.KB.fromBytes(KB), is(1.0));

    assertThat(StorageUnit.KB.toKBs(10), is(10.0));
    assertThat(StorageUnit.KB.toMBs(3.0 * 1024.0), is(3.0));
    assertThat(StorageUnit.KB.toGBs(1073741824), is(1024.0));
    assertThat(StorageUnit.KB.toTBs(1073741824), is(1.0));
    assertThat(StorageUnit.KB.toPBs(1.0995116e+12), is(1.0));
    assertThat(StorageUnit.KB.toEBs(1.1258999e+15), is(1.0));
  }

  @Test
  public void testMBConversions() {
    assertThat(StorageUnit.MB.getShortName(), is("mb"));
    assertThat(StorageUnit.MB.getSuffixChar(), is("m"));
    assertThat(StorageUnit.MB.getLongName(), is("megabytes"));
    assertThat(StorageUnit.MB.toString(), is("megabytes"));
    assertThat(StorageUnit.MB.toMBs(1024),
        is(StorageUnit.MB.getDefault(1024)));



    assertThat(StorageUnit.MB.toBytes(1), is(MB));
    assertThat(StorageUnit.MB.fromBytes(MB), is(1.0));

    assertThat(StorageUnit.MB.toKBs(1), is(1024.0));
    assertThat(StorageUnit.MB.toMBs(10), is(10.0));

    assertThat(StorageUnit.MB.toGBs(44040192), is(43008.0));
    assertThat(StorageUnit.MB.toTBs(1073741824), is(1024.0));
    assertThat(StorageUnit.MB.toPBs(1073741824), is(1.0));
    assertThat(StorageUnit.MB.toEBs(1 * (EB/MB)), is(1.0));
  }

  @Test
  public void testGBConversions() {
    assertThat(StorageUnit.GB.getShortName(), is("gb"));
    assertThat(StorageUnit.GB.getSuffixChar(), is("g"));
    assertThat(StorageUnit.GB.getLongName(), is("gigabytes"));
    assertThat(StorageUnit.GB.toString(), is("gigabytes"));
    assertThat(StorageUnit.GB.toGBs(1024),
        is(StorageUnit.GB.getDefault(1024)));


    assertThat(StorageUnit.GB.toBytes(1), is(GB));
    assertThat(StorageUnit.GB.fromBytes(GB), is(1.0));

    assertThat(StorageUnit.GB.toKBs(1), is(1024.0 * 1024));
    assertThat(StorageUnit.GB.toMBs(10), is(10.0 * 1024));

    assertThat(StorageUnit.GB.toGBs(44040192.0), is(44040192.0));
    assertThat(StorageUnit.GB.toTBs(1073741824), is(1048576.0));
    assertThat(StorageUnit.GB.toPBs(1.07375e+9), is(1024.0078));
    assertThat(StorageUnit.GB.toEBs(1 * (EB/GB)), is(1.0));
  }

  @Test
  public void testTBConversions() {
    assertThat(StorageUnit.TB.getShortName(), is("tb"));
    assertThat(StorageUnit.TB.getSuffixChar(), is("t"));
    assertThat(StorageUnit.TB.getLongName(), is("terabytes"));
    assertThat(StorageUnit.TB.toString(), is("terabytes"));
    assertThat(StorageUnit.TB.toTBs(1024),
        is(StorageUnit.TB.getDefault(1024)));

    assertThat(StorageUnit.TB.toBytes(1), is(TB));
    assertThat(StorageUnit.TB.fromBytes(TB), is(1.0));

    assertThat(StorageUnit.TB.toKBs(1), is(1024.0 * 1024* 1024));
    assertThat(StorageUnit.TB.toMBs(10), is(10.0 * 1024 * 1024));

    assertThat(StorageUnit.TB.toGBs(44040192.0), is(45097156608.0));
    assertThat(StorageUnit.TB.toTBs(1073741824.0), is(1073741824.0));
    assertThat(StorageUnit.TB.toPBs(1024), is(1.0));
    assertThat(StorageUnit.TB.toEBs(1 * (EB/TB)), is(1.0));
  }

  @Test
  public void testPBConversions() {
    assertThat(StorageUnit.PB.getShortName(), is("pb"));
    assertThat(StorageUnit.PB.getSuffixChar(), is("p"));
    assertThat(StorageUnit.PB.getLongName(), is("petabytes"));
    assertThat(StorageUnit.PB.toString(), is("petabytes"));
    assertThat(StorageUnit.PB.toPBs(1024),
        is(StorageUnit.PB.getDefault(1024)));


    assertThat(StorageUnit.PB.toBytes(1), is(PB));
    assertThat(StorageUnit.PB.fromBytes(PB), is(1.0));

    assertThat(StorageUnit.PB.toKBs(1), is(PB/KB));
    assertThat(StorageUnit.PB.toMBs(10), is(10.0 * (PB / MB)));

    assertThat(StorageUnit.PB.toGBs(44040192.0),
        is(44040192.0 * PB/GB));
    assertThat(StorageUnit.PB.toTBs(1073741824.0),
        is(1073741824.0 * (PB/TB)));
    assertThat(StorageUnit.PB.toPBs(1024.0), is(1024.0));
    assertThat(StorageUnit.PB.toEBs(1024.0), is(1.0));
  }


  @Test
  public void testEBConversions() {
    assertThat(StorageUnit.EB.getShortName(), is("eb"));
    assertThat(StorageUnit.EB.getSuffixChar(), is("e"));

    assertThat(StorageUnit.EB.getLongName(), is("exabytes"));
    assertThat(StorageUnit.EB.toString(), is("exabytes"));
    assertThat(StorageUnit.EB.toEBs(1024),
        is(StorageUnit.EB.getDefault(1024)));

    assertThat(StorageUnit.EB.toBytes(1), is(EB));
    assertThat(StorageUnit.EB.fromBytes(EB), is(1.0));

    assertThat(StorageUnit.EB.toKBs(1), is(EB/KB));
    assertThat(StorageUnit.EB.toMBs(10), is(10.0 * (EB / MB)));

    assertThat(StorageUnit.EB.toGBs(44040192.0),
        is(44040192.0 * EB/GB));
    assertThat(StorageUnit.EB.toTBs(1073741824.0),
        is(1073741824.0 * (EB/TB)));
    assertThat(StorageUnit.EB.toPBs(1.0), is(1024.0));
    assertThat(StorageUnit.EB.toEBs(42.0), is(42.0));
  }


}
