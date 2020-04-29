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
package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;

/**
 * This class mainly test the MockDomainNameResolver comes working as expected.
 */
public class TestMockDomainNameResolver {

  private Configuration conf;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() {
    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_DOMAINNAME_RESOLVER_IMPL,
        MockDomainNameResolver.class.getName());
  }

  @Test
  public void testMockDomainNameResolverCanBeCreated() throws IOException {
    DomainNameResolver resolver = DomainNameResolverFactory.newInstance(
        conf, CommonConfigurationKeys.HADOOP_DOMAINNAME_RESOLVER_IMPL);
    InetAddress[] addrs = resolver.getAllByDomainName(
        MockDomainNameResolver.DOMAIN);

    assertEquals(2, addrs.length);
    assertEquals(MockDomainNameResolver.ADDR_1, addrs[0].getHostAddress());
    assertEquals(MockDomainNameResolver.ADDR_2, addrs[1].getHostAddress());
  }

  @Test
  public void testMockDomainNameResolverCanNotBeCreated()
      throws UnknownHostException {
    DomainNameResolver resolver = DomainNameResolverFactory.newInstance(
        conf, CommonConfigurationKeys.HADOOP_DOMAINNAME_RESOLVER_IMPL);
    exception.expect(UnknownHostException.class);
    resolver.getAllByDomainName(
        MockDomainNameResolver.UNKNOW_DOMAIN);
  }
}
