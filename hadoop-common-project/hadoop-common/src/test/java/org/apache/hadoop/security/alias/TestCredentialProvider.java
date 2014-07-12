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
package org.apache.hadoop.security.alias;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TestCredentialProvider {

  @Test
  public void testCredentialEntry() throws Exception {
    char[] key1 = new char[]{1,2,3,4};
    CredentialProvider.CredentialEntry obj = 
        new CredentialProvider.CredentialEntry("cred1", key1);
    assertEquals("cred1", obj.getAlias());
    assertArrayEquals(new char[]{1,2,3,4}, obj.getCredential());
  }

  @Test
  public void testUnnestUri() throws Exception {
    assertEquals(new Path("hdfs://nn.example.com/my/path"),
        ProviderUtils.unnestUri(new URI("myscheme://hdfs@nn.example.com/my/path")));
    assertEquals(new Path("hdfs://nn/my/path?foo=bar&baz=bat#yyy"),
        ProviderUtils.unnestUri(new URI("myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy")));
    assertEquals(new Path("inner://hdfs@nn1.example.com/my/path"),
        ProviderUtils.unnestUri(new URI("outer://inner@hdfs@nn1.example.com/my/path")));
    assertEquals(new Path("user:///"),
        ProviderUtils.unnestUri(new URI("outer://user/")));
  }
}
