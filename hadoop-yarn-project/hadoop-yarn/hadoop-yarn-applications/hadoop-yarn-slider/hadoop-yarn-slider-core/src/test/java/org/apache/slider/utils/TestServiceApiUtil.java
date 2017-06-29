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
package org.apache.slider.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Artifact;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.Resource;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.persist.JsonSerDeser;
import org.apache.slider.util.RestApiConstants;
import org.apache.slider.util.RestApiErrorMessages;
import org.apache.slider.util.ServiceApiUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.slider.util.RestApiConstants.DEFAULT_COMPONENT_NAME;
import static org.apache.slider.util.RestApiConstants.DEFAULT_UNLIMITED_LIFETIME;
import static org.apache.slider.util.RestApiErrorMessages.*;
import static org.apache.slider.util.RestApiErrorMessages.ERROR_CONTAINERS_COUNT_INVALID;
import static org.apache.slider.util.RestApiErrorMessages.ERROR_RESOURCE_PROFILE_NOT_SUPPORTED_YET;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test for ServiceApiUtil helper methods.
 */
public class TestServiceApiUtil {
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

    SliderFileSystem sfs = initMock(null);

    Application app = new Application();

    // no name
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no name");
    } catch (IllegalArgumentException e) {
      assertEquals(ERROR_APPLICATION_NAME_INVALID, e.getMessage());
    }

    // bad format name
    String[] badNames = {"4finance", "Finance", "finance@home", LEN_64_STR};
    for (String badName : badNames) {
      app.setName(badName);
      try {
        ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
        Assert.fail(EXCEPTION_PREFIX + "application with bad name " + badName);
      } catch (IllegalArgumentException e) {
        assertEquals(String.format(
            ERROR_APPLICATION_NAME_INVALID_FORMAT, badName), e.getMessage());
      }
    }

    // launch command not specified
    app.setName(LEN_64_STR);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DEFAULT_DNS);
      Assert.fail(EXCEPTION_PREFIX + "application with no launch command");
    } catch (IllegalArgumentException e) {
      assertEquals(RestApiErrorMessages.ERROR_ABSENT_LAUNCH_COMMAND,
          e.getMessage());
    }

    // launch command not specified
    app.setName(LEN_64_STR.substring(0, RegistryConstants
        .MAX_FQDN_LABEL_LENGTH));
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no launch command");
    } catch (IllegalArgumentException e) {
      assertEquals(RestApiErrorMessages.ERROR_ABSENT_LAUNCH_COMMAND,
          e.getMessage());
    }

    // resource not specified
    app.setLaunchCommand("sleep 3600");
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no resource");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_RESOURCE_FOR_COMP_INVALID,
          RestApiConstants.DEFAULT_COMPONENT_NAME), e.getMessage());
    }

    // memory not specified
    Resource res = new Resource();
    app.setResource(res);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no memory");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_RESOURCE_MEMORY_FOR_COMP_INVALID,
          RestApiConstants.DEFAULT_COMPONENT_NAME), e.getMessage());
    }

    // invalid no of cpus
    res.setMemory("100mb");
    res.setCpus(-2);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(
          EXCEPTION_PREFIX + "application with invalid no of cpus");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_RESOURCE_CPUS_FOR_COMP_INVALID_RANGE,
          RestApiConstants.DEFAULT_COMPONENT_NAME), e.getMessage());
    }

    // number of containers not specified
    res.setCpus(2);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no container count");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains(ERROR_CONTAINERS_COUNT_INVALID));
    }

    // specifying profile along with cpus/memory raises exception
    res.setProfile("hbase_finance_large");
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX
          + "application with resource profile along with cpus/memory");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(RestApiErrorMessages
              .ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_FOR_COMP_NOT_SUPPORTED,
          RestApiConstants.DEFAULT_COMPONENT_NAME),
          e.getMessage());
    }

    // currently resource profile alone is not supported.
    // TODO: remove the next test once resource profile alone is supported.
    res.setCpus(null);
    res.setMemory(null);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with resource profile only");
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
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "null number of containers");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ERROR_CONTAINERS_COUNT_INVALID));
    }

    // negative number of containers
    app.setNumberOfContainers(-1L);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "negative number of containers");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith(ERROR_CONTAINERS_COUNT_INVALID));
    }

    // everything valid here
    app.setNumberOfContainers(5L);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      LOG.error("application attributes specified should be valid here", e);
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }

  @Test
  public void testArtifacts() throws IOException {
    SliderFileSystem sfs = initMock(null);

    Application app = new Application();
    app.setName("name");
    Resource res = new Resource();
    app.setResource(res);
    res.setMemory("512M");
    app.setNumberOfContainers(3L);

    // no artifact id fails with default type
    Artifact artifact = new Artifact();
    app.setArtifact(artifact);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no artifact id");
    } catch (IllegalArgumentException e) {
      assertEquals(ERROR_ARTIFACT_ID_INVALID, e.getMessage());
    }

    // no artifact id fails with APPLICATION type
    artifact.setType(Artifact.TypeEnum.APPLICATION);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no artifact id");
    } catch (IllegalArgumentException e) {
      assertEquals(ERROR_ARTIFACT_ID_INVALID, e.getMessage());
    }

    // no artifact id fails with TARBALL type
    artifact.setType(Artifact.TypeEnum.TARBALL);
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with no artifact id");
    } catch (IllegalArgumentException e) {
      assertEquals(ERROR_ARTIFACT_ID_INVALID, e.getMessage());
    }

    // everything valid here
    artifact.setType(Artifact.TypeEnum.DOCKER);
    artifact.setId("docker.io/centos:centos7");
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      LOG.error("application attributes specified should be valid here", e);
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    // defaults assigned
    assertEquals(app.getComponents().get(0).getName(),
        DEFAULT_COMPONENT_NAME);
    assertEquals(app.getLifetime(), DEFAULT_UNLIMITED_LIFETIME);
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
    return comp;
  }

  private static Application createValidApplication(String compName) {
    Application app = new Application();
    app.setLaunchCommand("sleep 3600");
    app.setName("name");
    app.setResource(createValidResource());
    app.setNumberOfContainers(1L);
    if (compName != null) {
      app.addComponent(createValidComponent(compName));
    }
    return app;
  }

  private static SliderFileSystem initMock(Application ext) throws IOException {
    SliderFileSystem sfs = createNiceMock(SliderFileSystem.class);
    FileSystem mockFs = createNiceMock(FileSystem.class);
    JsonSerDeser<Application> jsonSerDeser = createNiceMock(JsonSerDeser
        .class);
    expect(sfs.getFileSystem()).andReturn(mockFs).anyTimes();
    expect(sfs.buildClusterDirPath(anyObject())).andReturn(
        new Path("cluster_dir_path")).anyTimes();
    if (ext != null) {
      expect(jsonSerDeser.load(anyObject(), anyObject())).andReturn(ext)
          .anyTimes();
    }
    replay(sfs, mockFs, jsonSerDeser);
    ServiceApiUtil.setJsonSerDeser(jsonSerDeser);
    return sfs;
  }

  @Test
  public void testExternalApplication() throws IOException {
    Application ext = createValidApplication("comp1");
    SliderFileSystem sfs = initMock(ext);

    Application app = createValidApplication(null);

    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.APPLICATION);
    artifact.setId("id");
    app.setArtifact(artifact);

    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    assertEquals(1, app.getComponents().size());
    assertNotNull(app.getComponent("comp1"));
  }

  @Test
  public void testDuplicateComponents() throws IOException {
    SliderFileSystem sfs = initMock(null);

    String compName = "comp1";
    Application app = createValidApplication(compName);
    app.addComponent(createValidComponent(compName));

    // duplicate component name fails
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with component collision");
    } catch (IllegalArgumentException e) {
      assertEquals("Component name collision: " + compName, e.getMessage());
    }
  }

  @Test
  public void testExternalDuplicateComponent() throws IOException {
    Application ext = createValidApplication("comp1");
    SliderFileSystem sfs = initMock(ext);

    Application app = createValidApplication("comp1");
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.APPLICATION);
    artifact.setId("id");
    app.getComponent("comp1").setArtifact(artifact);

    // duplicate component name okay in the case of APPLICATION component
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }

  @Test
  public void testExternalComponent() throws IOException {
    Application ext = createValidApplication("comp1");
    SliderFileSystem sfs = initMock(ext);

    Application app = createValidApplication("comp2");
    Artifact artifact = new Artifact();
    artifact.setType(Artifact.TypeEnum.APPLICATION);
    artifact.setId("id");
    app.setArtifact(artifact);

    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    assertEquals(1, app.getComponents().size());
    // artifact ID not inherited from global
    assertNotNull(app.getComponent("comp2"));

    // set APPLICATION artifact id on component
    app.getComponent("comp2").setArtifact(artifact);

    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
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
    Component a = new Component().name("a");
    Component b = new Component().name("b");
    Component c = new Component().name("c");
    Component d = new Component().name("d").dependencies(Arrays.asList("c"));
    Component e = new Component().name("e").dependencies(Arrays.asList("b",
        "d"));

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

    SliderFileSystem sfs = initMock(null);
    Application application = createValidApplication(null);
    application.setComponents(Arrays.asList(c, d, e));
    try {
      ServiceApiUtil.validateAndResolveApplication(application, sfs);
      Assert.fail(EXCEPTION_PREFIX + "components with bad dependencies");
    } catch (IllegalArgumentException ex) {
      assertEquals(String.format(
          RestApiErrorMessages.ERROR_DEPENDENCY_INVALID, "b", "e"), ex
          .getMessage());
    }
  }

  @Test
  public void testInvalidComponent() throws IOException {
    SliderFileSystem sfs = initMock(null);
    testComponent(sfs, false);
    testComponent(sfs, true);
  }

  private static void testComponent(SliderFileSystem sfs, boolean unique)
      throws IOException {
    int maxLen = RegistryConstants.MAX_FQDN_LABEL_LENGTH;
    if (unique) {
      assertEquals(19, Long.toString(Long.MAX_VALUE).length());
      maxLen = maxLen - Long.toString(Long.MAX_VALUE).length();
    }
    String compName = LEN_64_STR.substring(0, maxLen + 1);
    Application app = createValidApplication(null);
    app.addComponent(createValidComponent(compName).uniqueComponentSupport(
        unique));

    // invalid component name fails if dns is enabled
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
      Assert.fail(EXCEPTION_PREFIX + "application with invalid component name");
    } catch (IllegalArgumentException e) {
      assertEquals(String.format(RestApiErrorMessages
          .ERROR_COMPONENT_NAME_INVALID, maxLen, compName), e.getMessage());
    }

    // does not fail if dns is disabled
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DEFAULT_DNS);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    compName = LEN_64_STR.substring(0, maxLen);
    app = createValidApplication(null);
    app.addComponent(createValidComponent(compName).uniqueComponentSupport(
        unique));

    // does not fail
    try {
      ServiceApiUtil.validateAndResolveApplication(app, sfs, CONF_DNS_ENABLED);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }
  }
}
