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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationIssue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult.ExplainEntry;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.ClassSerialisationConfig;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.IncludeRootJSONProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the JSON and XML wire format of the validate/v2 response.
 */
public class TestValidationResultInfo {

  private static final String CAPACITY_KEY =
      "yarn.scheduler.capacity.root.a.capacity";

  private static String toJson(ValidationResultInfo info) throws Exception {
    IncludeRootJSONProvider provider = new IncludeRootJSONProvider();
    assertTrue(provider.isWriteable(ValidationResultInfo.class,
        ValidationResultInfo.class, new Annotation[0],
        MediaType.APPLICATION_JSON_TYPE));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    provider.writeTo(info, ValidationResultInfo.class,
        ValidationResultInfo.class, new Annotation[0],
        MediaType.APPLICATION_JSON_TYPE, new MultivaluedHashMap<>(), out);
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  private static String toXml(ValidationResultInfo info) throws Exception {
    JAXBContext context = new JAXBContextResolver()
        .getContext(ValidationResultInfo.class);
    Marshaller marshaller = context.createMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
    StringWriter writer = new StringWriter();
    marshaller.marshal(info, writer);
    return writer.toString();
  }

  private static Map<String, List<ExplainEntry>> oneQueueExplain() {
    Map<String, List<ExplainEntry>> explain = new LinkedHashMap<>();
    explain.put("root.a", Collections.singletonList(
        new ExplainEntry(CAPACITY_KEY, "50", "PARENT", "root")));
    return explain;
  }

  @Test
  public void testRegisteredAsWrappedClass() {
    assertTrue(new ClassSerialisationConfig().getWrappedClasses()
        .contains(ValidationResultInfo.class));
  }

  @Test
  public void testValidWithoutIssuesKeepsEmptyIssuesWrapper() throws Exception {
    ValidationResultInfo info =
        new ValidationResultInfo(ValidationResult.valid());
    assertTrue(info.isValid());
    assertNull(info.getExplain());
    assertEquals("{\"validationResult\":{\"valid\":true,"
        + "\"issues\":{\"issue\":[]}}}", toJson(info));
    assertEquals("<validationResult><valid>true</valid><issues/>"
        + "</validationResult>", toXml(info));
  }

  @Test
  public void testSingleIssueIsAnArrayAndNullFieldsAreOmitted()
      throws Exception {
    ValidationResult result = new ValidationResult(Collections.singletonList(
        ValidationIssue.error(null, null, "invalid-mutation",
            "Queue root.x not found")));
    ValidationResultInfo info = new ValidationResultInfo(result);
    assertFalse(info.isValid());
    assertEquals("{\"validationResult\":{\"valid\":false,"
        + "\"issues\":{\"issue\":[{\"ruleId\":\"invalid-mutation\","
        + "\"severity\":\"ERROR\",\"message\":\"Queue root.x not found\"}]}}}",
        toJson(info));
    assertEquals("<validationResult><valid>false</valid><issues><issue>"
        + "<ruleId>invalid-mutation</ruleId><severity>ERROR</severity>"
        + "<message>Queue root.x not found</message></issue></issues>"
        + "</validationResult>", toXml(info));
  }

  @Test
  public void testFullShapeWithExplain() throws Exception {
    ValidationResult result = new ValidationResult(Arrays.asList(
        ValidationIssue.error("root.a", CAPACITY_KEY, "invalid-capacity",
            "bad"),
        ValidationIssue.warning("root.b", null, "queue-name", "odd")),
        oneQueueExplain());
    ValidationResultInfo info = new ValidationResultInfo(result);
    assertEquals("{\"validationResult\":{\"valid\":false,"
        + "\"issues\":{\"issue\":["
        + "{\"queuePath\":\"root.a\",\"propertyKey\":\"" + CAPACITY_KEY + "\","
        + "\"ruleId\":\"invalid-capacity\",\"severity\":\"ERROR\","
        + "\"message\":\"bad\"},"
        + "{\"queuePath\":\"root.b\",\"ruleId\":\"queue-name\","
        + "\"severity\":\"WARNING\",\"message\":\"odd\"}]},"
        + "\"explain\":{\"queue\":[{\"queuePath\":\"root.a\","
        + "\"property\":[{\"key\":\"" + CAPACITY_KEY + "\",\"value\":\"50\","
        + "\"source\":\"PARENT\",\"sourceDetail\":\"root\"}]}]}}}",
        toJson(info));
    assertEquals("<validationResult><valid>false</valid><issues>"
        + "<issue><queuePath>root.a</queuePath>"
        + "<propertyKey>" + CAPACITY_KEY + "</propertyKey>"
        + "<ruleId>invalid-capacity</ruleId><severity>ERROR</severity>"
        + "<message>bad</message></issue>"
        + "<issue><queuePath>root.b</queuePath><ruleId>queue-name</ruleId>"
        + "<severity>WARNING</severity><message>odd</message></issue>"
        + "</issues><explain><queue><queuePath>root.a</queuePath>"
        + "<property><key>" + CAPACITY_KEY + "</key><value>50</value>"
        + "<source>PARENT</source><sourceDetail>root</sourceDetail>"
        + "</property></queue></explain></validationResult>", toXml(info));
  }

  @Test
  public void testRequestedEmptyExplainIsEmitted() throws Exception {
    ValidationResult result = ValidationResult.valid()
        .withExplain(new LinkedHashMap<String, List<ExplainEntry>>());
    assertEquals("{\"validationResult\":{\"valid\":true,"
        + "\"issues\":{\"issue\":[]},\"explain\":{\"queue\":[]}}}",
        toJson(new ValidationResultInfo(result)));
  }

  @Test
  public void testWarningsOnlyIsValid() throws Exception {
    ValidationResult result = new ValidationResult(Collections.singletonList(
        ValidationIssue.warning("root.a", null, "queue-name", "odd")));
    assertTrue(new ValidationResultInfo(result).isValid());
  }
}
