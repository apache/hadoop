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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static org.apache.hadoop.yarn.api.records.LocalResourceType.*;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestLocalResource {

  static org.apache.hadoop.yarn.api.records.LocalResource getYarnResource(Path p, long size,
      long timestamp, LocalResourceType type, LocalResourceVisibility state, String pattern)
      throws URISyntaxException {
    org.apache.hadoop.yarn.api.records.LocalResource ret =
        RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
            org.apache.hadoop.yarn.api.records.LocalResource.class);
    ret.setResource(URL.fromURI(p.toUri()));
    ret.setSize(size);
    ret.setTimestamp(timestamp);
    ret.setType(type);
    ret.setVisibility(state);
    ret.setPattern(pattern);
    return ret;
  }

  static void checkEqual(LocalResourceRequest a, LocalResourceRequest b) {
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(0, a.compareTo(b));
    assertEquals(0, b.compareTo(a));
  }

  static void checkNotEqual(LocalResourceRequest a, LocalResourceRequest b) {
    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
    assertFalse(a.hashCode() == b.hashCode());
    assertFalse(0 == a.compareTo(b));
    assertFalse(0 == b.compareTo(a));
  }

  @Test
  public void testResourceEquality() throws URISyntaxException {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);

    long basetime = r.nextLong() >>> 2;
    org.apache.hadoop.yarn.api.records.LocalResource yA = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime, FILE, PUBLIC, null);
    org.apache.hadoop.yarn.api.records.LocalResource yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime, FILE, PUBLIC, null);
    final LocalResourceRequest a = new LocalResourceRequest(yA);
    LocalResourceRequest b = new LocalResourceRequest(yA);
    checkEqual(a, b);
    b = new LocalResourceRequest(yB);
    checkEqual(a, b);

    // ignore visibility
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime, FILE, PRIVATE, null);
    b = new LocalResourceRequest(yB);
    checkEqual(a, b);

    // ignore size
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), 0, basetime, FILE, PRIVATE, null);
    b = new LocalResourceRequest(yB);
    checkEqual(a, b);

    // note path
    yB = getYarnResource(
        new Path("hdfs://dingo.org:80/foobar"), 0, basetime, ARCHIVE, PUBLIC, null);
    b = new LocalResourceRequest(yB);
    checkNotEqual(a, b);

    // note type
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), 0, basetime, ARCHIVE, PUBLIC, null);
    b = new LocalResourceRequest(yB);
    checkNotEqual(a, b);

    // note timestamp
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), 0, basetime + 1, FILE, PUBLIC, null);
    b = new LocalResourceRequest(yB);
    checkNotEqual(a, b);

    // note pattern
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), 0, basetime + 1, FILE, PUBLIC, "^/foo/.*");
    b = new LocalResourceRequest(yB);
    checkNotEqual(a, b);
  }

  @Test
  public void testResourceOrder() throws URISyntaxException {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    long basetime = r.nextLong() >>> 2;
    org.apache.hadoop.yarn.api.records.LocalResource yA = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime, FILE, PUBLIC, "^/foo/.*");
    final LocalResourceRequest a = new LocalResourceRequest(yA);

    // Path primary
    org.apache.hadoop.yarn.api.records.LocalResource yB = getYarnResource(
        new Path("http://yak.org:80/foobaz"), -1, basetime, FILE, PUBLIC, "^/foo/.*");
    LocalResourceRequest b = new LocalResourceRequest(yB);
    assertTrue(0 > a.compareTo(b));

    // timestamp secondary
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime + 1, FILE, PUBLIC, "^/foo/.*");
    b = new LocalResourceRequest(yB);
    assertTrue(0 > a.compareTo(b));

    // type tertiary
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime, ARCHIVE, PUBLIC, "^/foo/.*");
    b = new LocalResourceRequest(yB);
    assertTrue(0 != a.compareTo(b)); // don't care about order, just ne
    
    // path 4th
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime, ARCHIVE, PUBLIC, "^/food/.*");
    b = new LocalResourceRequest(yB);
    assertTrue(0 != a.compareTo(b)); // don't care about order, just ne
    
    yB = getYarnResource(
        new Path("http://yak.org:80/foobar"), -1, basetime, ARCHIVE, PUBLIC, null);
    b = new LocalResourceRequest(yB);
    assertTrue(0 != a.compareTo(b)); // don't care about order, just ne
  }

}
