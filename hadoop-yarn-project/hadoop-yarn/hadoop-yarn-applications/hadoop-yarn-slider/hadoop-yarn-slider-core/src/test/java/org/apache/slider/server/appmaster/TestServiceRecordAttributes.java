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
package org.apache.slider.server.appmaster;

import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.conf.MapOperations;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TestServiceRecordAttributes extends Assert {

  @Test
  public void testAppConfigProvidedServiceRecordAttributes() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("slider.some.arbitrary.option", "arbitrary value");
    options.put("service.record.attribute.one_attribute", "one_attribute_value");
    options.put("service.record.attribute.second_attribute", "second_attribute_value");
    MapOperations serviceProps = new MapOperations(SliderKeys.COMPONENT_AM, options);
    options = new HashMap<>();
    options.put("some.component.attribute", "component_attribute_value");
    options.put("service.record.attribute.component_attribute", "component_attribute_value");
    MapOperations compProps = new MapOperations("TEST_COMP", options);

    SliderAppMaster appMaster = new SliderAppMaster();

    ServiceRecord appServiceRecord = new ServiceRecord();

    appMaster.setProvidedServiceRecordAttributes(serviceProps, appServiceRecord);

    assertNull("property should not be attribute",
               appServiceRecord.get("slider.some.arbitrary.option"));
    assertEquals("wrong value", "one_attribute_value",
                 appServiceRecord.get("one_attribute"));
    assertEquals("wrong value", "second_attribute_value",
                 appServiceRecord.get("second_attribute"));

    ServiceRecord compServiceRecord = new ServiceRecord();

    appMaster.setProvidedServiceRecordAttributes(compProps, compServiceRecord);

    assertNull("should not be attribute",
               compServiceRecord.get("some.component.attribute"));
    assertEquals("wrong value", "component_attribute_value",
                 compServiceRecord.get("component_attribute"));

  }
}
