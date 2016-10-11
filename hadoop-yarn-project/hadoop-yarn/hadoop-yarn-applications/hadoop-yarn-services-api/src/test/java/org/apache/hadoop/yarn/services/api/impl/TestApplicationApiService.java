/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.services.api.impl;

import static org.apache.hadoop.yarn.services.utils.RestApiConstants.*;
import static org.apache.hadoop.yarn.services.utils.RestApiErrorMessages.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.services.resource.Application;
import org.apache.hadoop.yarn.services.resource.Artifact;
import org.apache.hadoop.yarn.services.resource.Resource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for application life time monitor feature test.
 */
@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("org.apache.hadoop.yarn.services.api.impl.ApplicationApiService")
public class TestApplicationApiService {
  private static final Logger logger = LoggerFactory
      .getLogger(TestApplicationApiService.class);
  private static String EXCEPTION_PREFIX = "Should have thrown exception: ";
  private static String NO_EXCEPTION_PREFIX = "Should not have thrown exception: ";
  private ApplicationApiService appApiService;

  @Before
  public void setup() throws Exception {
     appApiService = new ApplicationApiService();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test(timeout = 90000)
  public void testValidateApplicationPostPayload() throws Exception {
    Application app = new Application();
    Map<String, String> compNameArtifactIdMap = new HashMap<>();

    // no name
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX + "application with no name");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_APPLICATION_NAME_INVALID, e.getMessage());
    }

    // bad format name
    String[] badNames = { "4finance", "Finance", "finance@home" };
    for (String badName : badNames) {
      app.setName(badName);
      try {
        appApiService.validateApplicationPostPayload(app,
            compNameArtifactIdMap);
        Assert.fail(EXCEPTION_PREFIX + "application with bad name " + badName);
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(ERROR_APPLICATION_NAME_INVALID_FORMAT,
            e.getMessage());
      }
    }

    // no artifact
    app.setName("finance_home");
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX + "application with no artifact");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_ARTIFACT_INVALID, e.getMessage());
    }

    // no artifact id
    Artifact artifact = new Artifact();
    app.setArtifact(artifact);
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX + "application with no artifact id");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_ARTIFACT_ID_INVALID, e.getMessage());
    }

    // if artifact is of type APPLICATION then everything is valid here
    artifact.setType(Artifact.TypeEnum.APPLICATION);
    artifact.setId("app.io/hbase:facebook_0.2");
    app.setNumberOfContainers(5l);
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
    } catch (IllegalArgumentException e) {
      logger.error("application attributes specified should be valid here", e);
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    // default-component, default-lifetime and the property component_type
    // should get assigned here
    Assert.assertEquals(app.getComponents().get(0).getName(),
        DEFAULT_COMPONENT_NAME);
    Assert.assertEquals(app.getLifetime(), DEFAULT_UNLIMITED_LIFETIME);
    Assert.assertEquals("Property not set",
        app.getConfiguration().getProperties().get(PROPERTY_COMPONENT_TYPE),
        COMPONENT_TYPE_EXTERNAL);

    // unset artifact type, default component and no of containers to test other
    // validation logic
    artifact.setType(null);
    app.setComponents(null);
    app.setNumberOfContainers(null);

    // resource not specified
    artifact.setId("docker.io/centos:centos7");
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX + "application with no resource");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_RESOURCE_INVALID, e.getMessage());
    }

    // memory not specified
    Resource res = new Resource();
    app.setResource(res);
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX + "application with no memory");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_RESOURCE_MEMORY_INVALID, e.getMessage());
    }

    // cpus not specified
    res.setMemory("2gb");
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX + "application with no cpu");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_RESOURCE_CPUS_INVALID, e.getMessage());
    }

    // invalid no of cpus
    res.setCpus(-2);
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(
          EXCEPTION_PREFIX + "application with invalid no of cpups");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_RESOURCE_CPUS_INVALID_RANGE, e.getMessage());
    }

    // number of containers not specified
    res.setCpus(2);
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(
          EXCEPTION_PREFIX + "application with no container count");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_CONTAINERS_COUNT_INVALID, e.getMessage());
    }

    // specifying profile along with cpus/memory raises exception
    res.setProfile("hbase_finance_large");
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX
          + "application with resource profile along with cpus/memory");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_NOT_SUPPORTED,
          e.getMessage());
    }

    // currently resource profile alone is not supported.
    // TODO: remove the next test once it is supported.
    res.setCpus(null);
    res.setMemory(null);
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
      Assert.fail(EXCEPTION_PREFIX
          + "application with resource profile only - NOT SUPPORTED");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ERROR_RESOURCE_PROFILE_NOT_SUPPORTED_YET,
          e.getMessage());
    }

    // unset profile here and add cpus/memory back
    res.setProfile(null);
    res.setCpus(2);
    res.setMemory("2gb");

    // everything valid here
    app.setNumberOfContainers(5l);
    try {
      appApiService.validateApplicationPostPayload(app,
          compNameArtifactIdMap);
    } catch (IllegalArgumentException e) {
      logger.error("application attributes specified should be valid here", e);
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    // Now test with components
  }
}
