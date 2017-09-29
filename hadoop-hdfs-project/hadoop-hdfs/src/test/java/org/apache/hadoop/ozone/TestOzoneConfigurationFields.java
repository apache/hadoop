/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.cblock.CBlockConfigKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.OzonePropertyTag;
import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.ozone.ksm.KSMConfigKeys;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Tests if configuration constants documented in ozone-defaults.xml.
 */
public class TestOzoneConfigurationFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String("ozone-default.xml");
    configurationClasses =
        new Class[] {OzoneConfigKeys.class, ScmConfigKeys.class,
            KSMConfigKeys.class, CBlockConfigKeys.class};
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;
  }

  @Test
  public void testOzoneTags() {
    Configuration config = new OzoneConfiguration();
    config.reloadConfiguration();

    // To load default resources
    config.get("ozone.enabled");
    assertEquals(87,
        config.getAllPropertiesByTag(OzonePropertyTag.OZONE).size());
    assertEquals(15, config.getAllPropertiesByTag(OzonePropertyTag.KSM)
        .size());
    assertEquals(6, config.getAllPropertiesByTag(OzonePropertyTag.SCM)
        .size());
    assertEquals(6, config.getAllPropertiesByTag(OzonePropertyTag.MANAGEMENT)
        .size());
    assertEquals(7, config.getAllPropertiesByTag(OzonePropertyTag.SECURITY)
        .size());
    assertEquals(9, config.getAllPropertiesByTag(OzonePropertyTag.PERFORMANCE)
        .size());
    assertEquals(1, config.getAllPropertiesByTag(OzonePropertyTag.DEBUG)
        .size());
    assertEquals(3, config.getAllPropertiesByTag(OzonePropertyTag.REQUIRED)
        .size());
    assertEquals(2, config.getAllPropertiesByTag(OzonePropertyTag.RATIS)
        .size());
  }

}