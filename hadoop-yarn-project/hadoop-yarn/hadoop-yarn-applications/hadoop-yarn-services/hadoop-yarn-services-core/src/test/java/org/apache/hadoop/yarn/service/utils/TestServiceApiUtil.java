/*
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
package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.service.api.records.PlacementConstraint;
import org.apache.hadoop.yarn.service.api.records.PlacementPolicy;
import org.apache.hadoop.yarn.service.api.records.PlacementScope;
import org.apache.hadoop.yarn.service.api.records.PlacementType;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.yarn.service.conf.RestApiConstants.DEFAULT_UNLIMITED_LIFETIME;
import static org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Test for ServiceApiUtil helper methods.
 */
public class TestServiceApiUtil extends ServiceTestUtils {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestServiceApiUtil.class);
  private static final String EXCEPTION_PREFIX = "Should have thrown " +
      "exception: ";
  private static final String NO_EXCEPTION_PREFIX = "Should not have thrown " +
      "exception: ";

  private static final String LEN_64_STR =
      "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01";

  private static final YarnConfiguration CONF_DEFAULT_DNS = new
      YarnConfiguration();
  private static final YarnConfiguration CONF_DNS_ENABLED = new
      YarnConfiguration();

  @BeforeClass
  public static void init() {
    CONF_DNS_ENABLED.setBoolean(RegistryConstants.KEY_DNS_ENABLED, true);
  }

  @Test(timeout = 90000)
  public void testResourceValidation() throws Exception {
    assertEquals(RegistryConstants.MAX_FQDN_LABEL_LENGTH + 1, LEN_64_STR
        .length());

    SliderFileSystem sfs = ServiceTestUtils.initMockFs();

    Service app = new Service();

    // no name
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with no name");
    } catch (IllegalArgumentException e) {
      assertEquals(ERROR_APPLICATION_NAME_INVALID, e.getMessage());
    }

    app.setName("test");
    // no version
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + " service with no version");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(ERROR_APPLICATION_VERSION_INVALID,
          app.getName()), e.getMessage());
    }

    app.setVersion("v1");
    // bad format name
    String[] badNames = {"4finance", "Finance", "finance@home", LEN_64_STR};
    for (String badName : badNames) {
      app.setName(badName);
      try {
        ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
        Assert.fail(EXCEPTION_PREFIX + "service with bad name " + badName);
      } catch (IllegalArgumentException e) {

      }
    }

    // launch command not specified
    app.setName(LEN_64_STR);
    Component comp = new Component().name("comp1");
    app.addComponent(comp);
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DEFAULT_DNS);
      Assert.fail(EXCEPTION_PREFIX + "service with no launch command");
    } catch (IllegalArgumentException e) {
      assertEquals(RestApiErrorMessages.ERROR_ABSENT_LAUNCH_COMMAND,
          e.getMessage());
    }

    // launch command not specified
    app.setName(LEN_64_STR.substring(0, RegistryConstants
        .MAX_FQDN_LABEL_LENGTH));
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with no launch command");
    } catch (IllegalArgumentException e) {
      assertEquals(RestApiErrorMessages.ERROR_ABSENT_LAUNCH_COMMAND,
          e.getMessage());
    }

    // memory not specified
    comp.setLaunchCommand("sleep 1");
    Resource res = new Resource();
    app.setResource(res);
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with no memory");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_RESOURCE_MEMORY_FOR_COMP_INVALID,
          comp.getName()), e.getMessage());
    }

    // invalid no of cpus
    res.setMemory("100mb");
    res.setCpus(-2);
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(
          EXCEPTION_PREFIX + "service with invalid no of cpus");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_RESOURCE_CPUS_FOR_COMP_INVALID_RANGE,
          comp.getName()), e.getMessage());
    }

    // number of containers not specified
    res.setCpus(2);
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with no container count");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains(ERROR_CONTAINERS_COUNT_INVALID));
    }

    // specifying profile along with cpus/memory raises exception
    res.setProfile("hbase_finance_large");
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX
          + "service with resource profile along with cpus/memory");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(RestApiErrorMessages
              .ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_FOR_COMP_NOT_SUPPORTED,
          comp.getName()),
          e.getMessage());
    }

    // currently resource profile alone is not supported.
    // TODO: remove the next test once resource profile alone is supported.
    res.setCpus(null);
    res.setMemory(null);
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with resource profile only");
    } catch (IllegalArgumentException e) {
      assertEquals(ERROR_RESOURCE_PROFILE_NOT_SUPPORTED_YET,
          e.getMessage());
    }

    // unset profile here and add cpus/memory back
    res.setProfile(null);
    res.setCpus(2);
    res.setMemory("2gb");

    // null number of containers
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "null number of containers");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ERROR_CONTAINERS_COUNT_INVALID));
    }
  }

  @Test
  public void testArtifacts() throws IOException {
    SliderFileSystem sfs = ServiceTestUtils.initMockFs();

    Service app = new Service();
    app.setName("service1");
    app.setVersion("v1");
    Resource res = new Resource();
    app.setResource(res);
    res.setMemory("512M");

    // no artifact id fails with default type
    Artifact artifact = new Artifact();
    app.setArtifact(artifact);
    String compName = "comp1";
    Component comp = ServiceTestUtils.createComponent(compName);

    app.setComponents(Collections.singletonList(comp));
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with no artifact id");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(ERROR_ARTIFACT_ID_FOR_COMP_INVALID, compName),
          e.getMessage());
    }

    // no artifact id fails with SERVICE type
    artifact.setType(Artifact.TypeEnum.SERVICE);
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with no artifact id");
    } catch (IllegalArgumentException e) {
      assertEquals(ERROR_ARTIFACT_ID_INVALID, e.getMessage());
    }

    // no artifact id fails with TARBALL type
    artifact.setType(Artifact.TypeEnum.TARBALL);
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with no artifact id");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(ERROR_ARTIFACT_ID_FOR_COMP_INVALID, compName),
          e.getMessage());
    }

    // everything valid here
    artifact.setType(Artifact.TypeEnum.DOCKER);
    artifact.setId("docker.io/centos:centos7");
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      LOG.error("service attributes specified should be valid here", e);
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    assertThat(app.getLifetime()).isEqualTo(DEFAULT_UNLIMITED_LIFETIME);
  }

  private static Resource createValidResource() {
    Resource res = new Resource();
    res.setMemory("512M");
    return res;
  }

  private static Component createValidComponent(String compName) {
    Component comp = new Component();
    comp.setName(compName);
    comp.setResource(createValidResource());
    comp.setNumberOfContainers(1L);
    comp.setLaunchCommand("sleep 1");
    return comp;
  }

  private static Service createValidApplication(String compName) {
    Service app = new Service();
    app.setName("name");
    app.setVersion("v1");
    app.setResource(createValidResource());
    if (compName != null) {
      app.addComponent(createValidComponent(compName));
    }
    return app;
  }

  @Test
  public void testExternalApplication() throws IOException {
    Service ext = createValidApplication("comp1");
    SliderFileSystem sfs = ServiceTestUtils.initMockFs(ext);

    Service app = createValidApplication(null);

    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.SERVICE);
    artifact.setId("id");
    app.setArtifact(artifact);
    app.addComponent(ServiceTestUtils.createComponent("comp2"));
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    assertEquals(1, app.getComponents().size());
    assertNotNull(app.getComponent("comp2"));
  }

  @Test
  public void testDuplicateComponents() throws IOException {
    SliderFileSystem sfs = ServiceTestUtils.initMockFs();

    String compName = "comp1";
    Service app = createValidApplication(compName);
    app.addComponent(createValidComponent(compName));

    // duplicate component name fails
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with component collision");
    } catch (IllegalArgumentException e) {
      assertEquals("Component name collision: " + compName, e.getMessage());
    }
  }

  @Test
  public void testComponentNameSameAsServiceName() throws IOException {
    SliderFileSystem sfs = ServiceTestUtils.initMockFs();
    Service app = new Service();
    app.setName("test");
    app.setVersion("v1");
    app.addComponent(createValidComponent("test"));

    //component name same as service name
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "component name matches service name");
    } catch (IllegalArgumentException e) {
      assertEquals("Component name test must not be same as service name test",
          e.getMessage());
    }
  }

  @Test
  public void testExternalDuplicateComponent() throws IOException {
    Service ext = createValidApplication("comp1");
    SliderFileSystem sfs = ServiceTestUtils.initMockFs(ext);

    Service app = createValidApplication("comp1");
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.SERVICE);
    artifact.setId("id");
    app.getComponent("comp1").setArtifact(artifact);

    // duplicate component name okay in the case of SERVICE component
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }

  @Test
  public void testExternalComponent() throws IOException {
    Service ext = createValidApplication("comp1");
    SliderFileSystem sfs = ServiceTestUtils.initMockFs(ext);

    Service app = createValidApplication("comp2");
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.SERVICE);
    artifact.setId("id");
    app.setArtifact(artifact);

    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    assertEquals(1, app.getComponents().size());
    // artifact ID not inherited from global
    assertNotNull(app.getComponent("comp2"));

    // set SERVICE artifact id on component
    app.getComponent("comp2").setArtifact(artifact);

    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    assertEquals(1, app.getComponents().size());
    // original component replaced by external component
    assertNotNull(app.getComponent("comp1"));
  }

  public static void verifyDependencySorting(List<Component> components,
      Component... expectedSorting) {
    Collection<Component> actualSorting = ServiceApiUtil.sortByDependencies(
        components);
    assertEquals(expectedSorting.length, actualSorting.size());
    int i = 0;
    for (Component component : actualSorting) {
      assertEquals(expectedSorting[i++], component);
    }
  }

  @Test
  public void testDependencySorting() throws IOException {
    Component a = ServiceTestUtils.createComponent("a");
    Component b = ServiceTestUtils.createComponent("b");
    Component c = ServiceTestUtils.createComponent("c");
    Component d =
        ServiceTestUtils.createComponent("d").dependencies(Arrays.asList("c"));
    Component e = ServiceTestUtils.createComponent("e")
        .dependencies(Arrays.asList("b", "d"));

    verifyDependencySorting(Arrays.asList(a, b, c), a, b, c);
    verifyDependencySorting(Arrays.asList(c, a, b), c, a, b);
    verifyDependencySorting(Arrays.asList(a, b, c, d, e), a, b, c, d, e);
    verifyDependencySorting(Arrays.asList(e, d, c, b, a), c, b, a, d, e);

    c.setDependencies(Arrays.asList("e"));
    try {
      verifyDependencySorting(Arrays.asList(a, b, c, d, e));
      Assert.fail(EXCEPTION_PREFIX + "components with dependency cycle");
    } catch (IllegalArgumentException ex) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_DEPENDENCY_CYCLE, Arrays.asList(c, d,
              e)), ex.getMessage());
    }

    SliderFileSystem sfs = ServiceTestUtils.initMockFs();
    Service service = createValidApplication(null);
    service.setComponents(Arrays.asList(c, d, e));
    try {
      ServiceApiUtil.validateAndResolveService(service, sfs,
          CONF_DEFAULT_DNS);
      Assert.fail(EXCEPTION_PREFIX + "components with bad dependencies");
    } catch (IllegalArgumentException ex) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_DEPENDENCY_INVALID, "b", "e"), ex
          .getMessage());
    }
  }

  @Test
  public void testInvalidComponent() throws IOException {
    SliderFileSystem sfs = ServiceTestUtils.initMockFs();
    testComponent(sfs);
  }

  @Test
  public void testValidateCompName() {
    String[] invalidNames = {
        "EXAMPLE", // UPPER case not allowed
        "example_app" // underscore not allowed.
    };
    for (String name : invalidNames) {
      try {
        ServiceApiUtil.validateNameFormat(name, new Configuration());
        Assert.fail();
      } catch (IllegalArgumentException ex) {
        ex.printStackTrace();
      }
    }
  }

  private static void testComponent(SliderFileSystem sfs)
      throws IOException {
    int maxLen = RegistryConstants.MAX_FQDN_LABEL_LENGTH;
    assertEquals(19, Long.toString(Long.MAX_VALUE).length());
    maxLen = maxLen - Long.toString(Long.MAX_VALUE).length();

    String compName = LEN_64_STR.substring(0, maxLen + 1);
    Service app = createValidApplication(null);
    app.addComponent(createValidComponent(compName));

    // invalid component name fails if dns is enabled
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "service with invalid component name");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(RestApiErrorMessages
          .ERROR_COMPONENT_NAME_INVALID, maxLen, compName), e.getMessage());
    }

    // does not fail if dns is disabled
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DEFAULT_DNS);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    compName = LEN_64_STR.substring(0, maxLen);
    app = createValidApplication(null);
    app.addComponent(createValidComponent(compName));

    // does not fail
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }

  @Test
  public void testPlacementPolicy() throws IOException {
    SliderFileSystem sfs = ServiceTestUtils.initMockFs();
    Service app = createValidApplication("comp-a");
    Component comp = app.getComponents().get(0);
    PlacementPolicy pp = new PlacementPolicy();
    PlacementConstraint pc = new PlacementConstraint();
    pc.setName("CA1");
    pp.setConstraints(Collections.singletonList(pc));
    comp.setPlacementPolicy(pp);

    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "constraint with no type");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_PLACEMENT_POLICY_CONSTRAINT_TYPE_NULL,
          "CA1 ", "comp-a"), e.getMessage());
    }

    // Set the type
    pc.setType(PlacementType.ANTI_AFFINITY);

    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "constraint with no scope");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_PLACEMENT_POLICY_CONSTRAINT_SCOPE_NULL,
          "CA1 ", "comp-a"), e.getMessage());
    }

    // Set the scope
    pc.setScope(PlacementScope.NODE);

    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "constraint with no tag(s)");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_PLACEMENT_POLICY_CONSTRAINT_TAGS_NULL,
          "CA1 ", "comp-a"), e.getMessage());
    }

    // Set a target tag - but an invalid one
    pc.setTargetTags(Collections.singletonList("comp-invalid"));

    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "constraint with invalid tag name");
    } catch (IllegalArgumentException e) {
      assertEquals(
          String.format(
              RestApiErrorMessages.ERROR_PLACEMENT_POLICY_TAG_NAME_NOT_SAME,
              "comp-invalid", "comp-a", "comp-a", "comp-a"),
          e.getMessage());
    }

    // Set valid target tags now
    pc.setTargetTags(Collections.singletonList("comp-a"));

    // Finally it should succeed
    try {
      ServiceApiUtil.validateAndResolveService(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }

  @Test
  public void testKerberosPrincipal() throws IOException {
    SliderFileSystem sfs = ServiceTestUtils.initMockFs();
    Service app = createValidApplication("comp-a");
    KerberosPrincipal kp = new KerberosPrincipal();
    kp.setKeytab("file:///tmp/a.keytab");
    kp.setPrincipalName("user/_HOST@domain.com");
    app.setKerberosPrincipal(kp);

    // This should succeed
    try {
      ServiceApiUtil.validateKerberosPrincipal(app.getKerberosPrincipal());
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    // Keytab with no URI scheme should succeed too
    kp.setKeytab("/some/path");
    try {
      ServiceApiUtil.validateKerberosPrincipal(app.getKerberosPrincipal());
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }

  @Test
  public void testKerberosPrincipalNameFormat() throws IOException {
    Service app = createValidApplication("comp-a");
    KerberosPrincipal kp = new KerberosPrincipal();
    kp.setPrincipalName("user@domain.com");
    app.setKerberosPrincipal(kp);

    try {
      ServiceApiUtil.validateKerberosPrincipal(app.getKerberosPrincipal());
      Assert.fail(EXCEPTION_PREFIX + "service with invalid principal name " +
          "format.");
    } catch (IllegalArgumentException e) {
      assertEquals(
          String.format(
              RestApiErrorMessages.ERROR_KERBEROS_PRINCIPAL_NAME_FORMAT,
              kp.getPrincipalName()),
          e.getMessage());
    }

    kp.setPrincipalName("user/_HOST@domain.com");
    try {
      ServiceApiUtil.validateKerberosPrincipal(app.getKerberosPrincipal());
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    kp.setPrincipalName(null);
    kp.setKeytab(null);
    try {
      ServiceApiUtil.validateKerberosPrincipal(app.getKerberosPrincipal());
    } catch (NullPointerException e) {
        Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }

  @Test
  public void testResolveCompsDependency() {
    Service service = createExampleApplication();
    List<String> dependencies = new ArrayList<String>();
    dependencies.add("compb");
    Component compa = createComponent("compa");
    compa.setDependencies(dependencies);
    Component compb = createComponent("compb");
    service.addComponent(compa);
    service.addComponent(compb);
    List<String> order = ServiceApiUtil.resolveCompsDependency(service);
    List<String> expected = new ArrayList<String>();
    expected.add("compb");
    expected.add("compa");
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals("Components are not equal.", expected.get(i),
          order.get(i));
    }
  }

  @Test
  public void testResolveCompsDependencyReversed() {
    Service service = createExampleApplication();
    List<String> dependencies = new ArrayList<String>();
    dependencies.add("compa");
    Component compa = createComponent("compa");
    Component compb = createComponent("compb");
    compb.setDependencies(dependencies);
    service.addComponent(compa);
    service.addComponent(compb);
    List<String> order = ServiceApiUtil.resolveCompsDependency(service);
    List<String> expected = new ArrayList<String>();
    expected.add("compa");
    expected.add("compb");
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals("Components are not equal.", expected.get(i),
          order.get(i));
    }
  }

  @Test
  public void testResolveCompsCircularDependency() {
    Service service = createExampleApplication();
    List<String> dependencies = new ArrayList<String>();
    List<String> dependencies2 = new ArrayList<String>();
    dependencies.add("compb");
    dependencies2.add("compa");
    Component compa = createComponent("compa");
    compa.setDependencies(dependencies);
    Component compb = createComponent("compb");
    compa.setDependencies(dependencies2);
    service.addComponent(compa);
    service.addComponent(compb);
    List<String> order = ServiceApiUtil.resolveCompsDependency(service);
    List<String> expected = new ArrayList<String>();
    expected.add("compa");
    expected.add("compb");
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals("Components are not equal.", expected.get(i),
          order.get(i));
    }
  }

  @Test
  public void testResolveNoCompsDependency() {
    Service service = createExampleApplication();
    Component compa = createComponent("compa");
    Component compb = createComponent("compb");
    service.addComponent(compa);
    service.addComponent(compb);
    List<String> order = ServiceApiUtil.resolveCompsDependency(service);
    List<String> expected = new ArrayList<String>();
    expected.add("compa");
    expected.add("compb");
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals("Components are not equal.", expected.get(i),
          order.get(i));
    }
  }

  @Test(timeout = 1500)
  public void testNoServiceDependencies() {
    Service service = createExampleApplication();
    Component compa = createComponent("compa");
    Component compb = createComponent("compb");
    service.addComponent(compa);
    service.addComponent(compb);
    List<String> dependencies = new ArrayList<String>();
    service.setDependencies(dependencies);
    ServiceApiUtil.checkServiceDependencySatisified(service);
  }

  @Test
  public void testServiceDependencies() {
    Thread thread = new Thread() {
      @Override
      public void run() {
        Service service = createExampleApplication();
        Component compa = createComponent("compa");
        Component compb = createComponent("compb");
        service.addComponent(compa);
        service.addComponent(compb);
        List<String> dependencies = new ArrayList<String>();
        dependencies.add("abc");
        service.setDependencies(dependencies);
        Service dependent = createExampleApplication();
        dependent.setState(ServiceState.STOPPED);
        ServiceApiUtil.checkServiceDependencySatisified(service);
      }
    };
    thread.start();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
    Assert.assertTrue(thread.isAlive());
  }

  @Test
  public void testJvmOpts() throws Exception {
    String invalidJvmOpts = "`ping -c 3 example.com`";
    intercept(IllegalArgumentException.class,
        "Invalid character in yarn.service.am.java.opts.",
        () -> ServiceApiUtil.validateJvmOpts(invalidJvmOpts));
    String validJvmOpts = "-Dyarn.service.am.java.opts=-Xmx768m "
        + "-Djava.security.auth.login.config=/opt/hadoop/etc/jaas-zk.conf";
    try {
      ServiceApiUtil.validateJvmOpts(validJvmOpts);
    } catch (Exception ex) {
      fail("Invalid character in yarn.service.am.java.opts.");
    }
  }

  public static Service createExampleApplication() {

    Service exampleApp = new Service();
    exampleApp.setName("example-app");
    exampleApp.setVersion("v1");
    return exampleApp;
  }
}
