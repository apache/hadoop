/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.service.api.records.Configuration;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.junit.Test;

import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.yarn.submarine.utils.EnvironmentUtilities.ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is to test {@link EnvironmentUtilities}.
 */
public class TestEnvironmentUtilities {
  private Service createServiceWithEmptyEnvVars() {
    return createServiceWithEnvVars(Maps.newHashMap());
  }

  private Service createServiceWithEnvVars(Map<String, String> envVars) {
    Service service = mock(Service.class);
    Configuration config = mock(Configuration.class);
    when(config.getEnv()).thenReturn(envVars);
    when(service.getConfiguration()).thenReturn(config);

    return service;
  }

  private void validateDefaultEnvVars(Map<String, String> resultEnvs) {
    assertEquals("/etc/passwd:/etc/passwd:ro",
        resultEnvs.get(ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME));
  }

  private org.apache.hadoop.conf.Configuration
  createYarnConfigWithSecurityValue(String value) {
    org.apache.hadoop.conf.Configuration mockConfig =
        mock(org.apache.hadoop.conf.Configuration.class);
    when(mockConfig.get(HADOOP_SECURITY_AUTHENTICATION)).thenReturn(value);
    return mockConfig;
  }

  @Test
  public void testGetValueOfNullEnvVar() {
    assertEquals("", EnvironmentUtilities.getValueOfEnvironment(null));
  }

  @Test
  public void testGetValueOfEmptyEnvVar() {
    assertEquals("", EnvironmentUtilities.getValueOfEnvironment(""));
  }

  @Test
  public void testGetValueOfEnvVarJustAnEqualsSign() {
    assertEquals("", EnvironmentUtilities.getValueOfEnvironment("="));
  }

  @Test
  public void testGetValueOfEnvVarWithoutValue() {
    assertEquals("", EnvironmentUtilities.getValueOfEnvironment("a="));
  }

  @Test
  public void testGetValueOfEnvVarValidFormat() {
    assertEquals("bbb", EnvironmentUtilities.getValueOfEnvironment("a=bbb"));
  }

  @Test
  public void testHandleServiceEnvWithNullMap() {
    Service service = createServiceWithEmptyEnvVars();
    org.apache.hadoop.conf.Configuration yarnConfig =
        mock(org.apache.hadoop.conf.Configuration.class);
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, null);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(1, resultEnvs.size());
    validateDefaultEnvVars(resultEnvs);
  }

  @Test
  public void testHandleServiceEnvWithEmptyMap() {
    Service service = createServiceWithEmptyEnvVars();
    org.apache.hadoop.conf.Configuration yarnConfig =
        mock(org.apache.hadoop.conf.Configuration.class);
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, null);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(1, resultEnvs.size());
    validateDefaultEnvVars(resultEnvs);
  }

  @Test
  public void testHandleServiceEnvWithYarnConfigSecurityValueNonKerberos() {
    Service service = createServiceWithEmptyEnvVars();
    org.apache.hadoop.conf.Configuration yarnConfig =
        createYarnConfigWithSecurityValue("nonkerberos");
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, null);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(1, resultEnvs.size());
    validateDefaultEnvVars(resultEnvs);
  }

  @Test
  public void testHandleServiceEnvWithYarnConfigSecurityValueKerberos() {
    Service service = createServiceWithEmptyEnvVars();
    org.apache.hadoop.conf.Configuration yarnConfig =
        createYarnConfigWithSecurityValue("kerberos");
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, null);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(1, resultEnvs.size());
    assertEquals("/etc/passwd:/etc/passwd:ro,/etc/krb5.conf:/etc/krb5.conf:ro",
        resultEnvs.get(ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME));
  }

  @Test
  public void testHandleServiceEnvWithExistingEnvsAndValidNewEnvs() {
    Map<String, String> existingEnvs = Maps.newHashMap(
        ImmutableMap.<String, String>builder().
            put("a", "1").
            put("b", "2").
            build());
    ImmutableList<String> newEnvs = ImmutableList.of("c=3", "d=4");

    Service service = createServiceWithEnvVars(existingEnvs);
    org.apache.hadoop.conf.Configuration yarnConfig =
        createYarnConfigWithSecurityValue("kerberos");
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, newEnvs);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(5, resultEnvs.size());
    assertEquals("/etc/passwd:/etc/passwd:ro,/etc/krb5.conf:/etc/krb5.conf:ro",
        resultEnvs.get(ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME));
    assertEquals("1", resultEnvs.get("a"));
    assertEquals("2", resultEnvs.get("b"));
    assertEquals("3", resultEnvs.get("c"));
    assertEquals("4", resultEnvs.get("d"));
  }

  @Test
  public void testHandleServiceEnvWithExistingEnvsAndNewEnvsWithoutEquals() {
    Map<String, String> existingEnvs = Maps.newHashMap(
        ImmutableMap.<String, String>builder().
            put("a", "1").
            put("b", "2").
            build());
    ImmutableList<String> newEnvs = ImmutableList.of("c3", "d4");

    Service service = createServiceWithEnvVars(existingEnvs);
    org.apache.hadoop.conf.Configuration yarnConfig =
        createYarnConfigWithSecurityValue("kerberos");
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, newEnvs);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(5, resultEnvs.size());
    assertEquals("/etc/passwd:/etc/passwd:ro,/etc/krb5.conf:/etc/krb5.conf:ro",
        resultEnvs.get(ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME));
    assertEquals("1", resultEnvs.get("a"));
    assertEquals("2", resultEnvs.get("b"));
    assertEquals("", resultEnvs.get("c3"));
    assertEquals("", resultEnvs.get("d4"));
  }

  @Test
  public void testHandleServiceEnvWithExistingEnvVarKey() {
    Map<String, String> existingEnvs = Maps.newHashMap(
        ImmutableMap.<String, String>builder().
            put("a", "1").
            put("b", "2").
            build());
    ImmutableList<String> newEnvs = ImmutableList.of("a=33", "c=44");

    Service service = createServiceWithEnvVars(existingEnvs);
    org.apache.hadoop.conf.Configuration yarnConfig =
        createYarnConfigWithSecurityValue("kerberos");
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, newEnvs);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(4, resultEnvs.size());
    assertEquals("/etc/passwd:/etc/passwd:ro,/etc/krb5.conf:/etc/krb5.conf:ro",
        resultEnvs.get(ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME));
    assertEquals("1:33", resultEnvs.get("a"));
    assertEquals("2", resultEnvs.get("b"));
    assertEquals("44", resultEnvs.get("c"));
  }

  @Test
  public void testHandleServiceEnvWithExistingEnvVarKeyMultipleTimes() {
    Map<String, String> existingEnvs = Maps.newHashMap(
        ImmutableMap.<String, String>builder().
            put("a", "1").
            put("b", "2").
            build());
    ImmutableList<String> newEnvs = ImmutableList.of("a=33", "a=44");

    Service service = createServiceWithEnvVars(existingEnvs);
    org.apache.hadoop.conf.Configuration yarnConfig =
        createYarnConfigWithSecurityValue("kerberos");
    EnvironmentUtilities.handleServiceEnvs(service, yarnConfig, newEnvs);

    Map<String, String> resultEnvs = service.getConfiguration().getEnv();
    assertEquals(3, resultEnvs.size());
    assertEquals("/etc/passwd:/etc/passwd:ro,/etc/krb5.conf:/etc/krb5.conf:ro",
        resultEnvs.get(ENV_DOCKER_MOUNTS_FOR_CONTAINER_RUNTIME));
    assertEquals("1:33:44", resultEnvs.get("a"));
    assertEquals("2", resultEnvs.get("b"));
  }

}