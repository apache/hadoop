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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.annotation.Annotation;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;

import org.junit.Test;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;

/**
 * Basic checks that {@link ExcludeRootJSONProvider} selects JSON for unlisted
 * {@link XmlRootElement} types (fallback) and respects wrapped / unwrapped lists.
 */
public class TestExcludeRootJSONProviderFallback {

  /** Not on {@link ClassSerialisationConfig} lists; for this test only. */
  @XmlRootElement(name = "testRoot")
  public static final class UnlistedJaxbRoot {
    private String value;

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  private final ExcludeRootJSONProvider provider = new ExcludeRootJSONProvider();

  @Test
  public void testFallbackAcceptsUnlistedJaxbRootElement() {
    assertTrue(isJsonCompatible(UnlistedJaxbRoot.class));
  }

  @Test
  public void testExplicitUnwrappedStillAccepted() {
    assertTrue(isJsonCompatible(NewApplication.class));
  }

  @Test
  public void testWrappedTypeRejected() {
    assertFalse(isJsonCompatible(ClusterInfo.class));
  }

  @Test
  public void testNonJaxbTypeRejected() {
    assertFalse(isJsonCompatible(String.class));
  }

  private boolean isJsonCompatible(Class<?> type) {
    return provider.isReadable(type, type, new Annotation[0], MediaType.APPLICATION_JSON_TYPE)
        && provider.isWriteable(type, type, new Annotation[0], MediaType.APPLICATION_JSON_TYPE);
  }
}
