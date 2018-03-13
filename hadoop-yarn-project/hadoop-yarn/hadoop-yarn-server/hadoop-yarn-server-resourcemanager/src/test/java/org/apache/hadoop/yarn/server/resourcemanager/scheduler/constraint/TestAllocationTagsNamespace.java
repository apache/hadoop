package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint; /**
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
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespace;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.TargetApplications;
import org.apache.hadoop.yarn.exceptions.InvalidAllocationTagException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link AllocationTagNamespace}.
 */
public class TestAllocationTagsNamespace {

  @Test
  public void testNamespaceParse() throws InvalidAllocationTagException {
    AllocationTagNamespace namespace;

    String namespaceStr = "self";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    Assert.assertTrue(namespace.isIntraApp());

    namespaceStr = "not-self";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    Assert.assertTrue(namespace.isNotSelf());

    namespaceStr = "all";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    Assert.assertTrue(namespace.isGlobal());

    namespaceStr = "app-label";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    Assert.assertTrue(namespace.isAppLabel());

    ApplicationId applicationId = ApplicationId.newInstance(12345, 1);
    namespaceStr = "app-id/" + applicationId.toString();
    namespace = AllocationTagNamespace.parse(namespaceStr);
    Assert.assertTrue(namespace.isSingleInterApp());

    // Invalid app-id namespace syntax, invalid app ID.
    try {
      namespaceStr = "app-id/apppppp_12345_99999";
      AllocationTagNamespace.parse(namespaceStr);
      Assert.fail("Parsing should fail as the given app ID is invalid");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidAllocationTagException);
      Assert.assertTrue(e.getMessage().startsWith(
          "Invalid application ID for app-id"));
    }

    // Invalid app-id namespace syntax, missing app ID.
    try {
      namespaceStr = "app-id";
      AllocationTagNamespace.parse(namespaceStr);
      Assert.fail("Parsing should fail as the given namespace"
          + " is missing application ID");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidAllocationTagException);
      Assert.assertTrue(e.getMessage().startsWith(
          "Missing the application ID in the namespace string"));
    }

    // Invalid namespace type.
    try {
      namespaceStr = "non_exist_ns";
      AllocationTagNamespace.parse(namespaceStr);
      Assert.fail("Parsing should fail as the giving type is not supported.");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidAllocationTagException);
      Assert.assertTrue(e.getMessage().startsWith(
          "Invalid namespace prefix"));
    }
  }

  @Test
  public void testNamespaceEvaluation() throws InvalidAllocationTagException {
    AllocationTagNamespace namespace;
    TargetApplications targetApplications;
    ApplicationId app1 = ApplicationId.newInstance(10000, 1);
    ApplicationId app2 = ApplicationId.newInstance(10000, 2);
    ApplicationId app3 = ApplicationId.newInstance(10000, 3);
    ApplicationId app4 = ApplicationId.newInstance(10000, 4);
    ApplicationId app5 = ApplicationId.newInstance(10000, 5);

    // Ensure eval is called before using the scope.
    String namespaceStr = "self";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    try {
      namespace.getNamespaceScope();
      Assert.fail("Call getNamespaceScope before evaluate is not allowed.");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalStateException);
      Assert.assertTrue(e.getMessage().contains(
          "Evaluate must be called before a namespace can be consumed."));
    }

    namespaceStr = "self";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    targetApplications = new TargetApplications(app1, ImmutableSet.of(app1));
    namespace.evaluate(targetApplications);
    Assert.assertEquals(1, namespace.getNamespaceScope().size());
    Assert.assertEquals(app1, namespace.getNamespaceScope().iterator().next());

    namespaceStr = "not-self";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    targetApplications = new TargetApplications(app1, ImmutableSet.of(app1));
    namespace.evaluate(targetApplications);
    Assert.assertEquals(0, namespace.getNamespaceScope().size());

    targetApplications = new TargetApplications(app1,
        ImmutableSet.of(app1, app2, app3));
    namespace.evaluate(targetApplications);
    Assert.assertEquals(2, namespace.getNamespaceScope().size());
    Assert.assertFalse(namespace.getNamespaceScope().contains(app1));

    namespaceStr = "all";
    namespace = AllocationTagNamespace.parse(namespaceStr);
    targetApplications = new TargetApplications(null,
        ImmutableSet.of(app1, app2));
    namespace.evaluate(targetApplications);
    Assert.assertEquals(2, namespace.getNamespaceScope().size());

    namespaceStr = "app-id/" + app2.toString();
    namespace = AllocationTagNamespace.parse(namespaceStr);
    targetApplications = new TargetApplications(app1,
        ImmutableSet.of(app1, app2, app3, app4, app5));
    namespace.evaluate(targetApplications);
    Assert.assertEquals(1, namespace.getNamespaceScope().size());
    Assert.assertEquals(app2, namespace.getNamespaceScope().iterator().next());
  }
}