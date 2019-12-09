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
package org.apache.hadoop.conf;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Properties;

/**
 * Created 21-Jan-2009 13:42:36
 */

public class TestConfigurationSubclass {
  private static final String EMPTY_CONFIGURATION_XML
          = "/org/apache/hadoop/conf/empty-configuration.xml";


  @Test
  public void testGetProps() {
    SubConf conf = new SubConf(true);
    Properties properties = conf.getProperties();
    assertNotNull("hadoop.tmp.dir is not set",
            properties.getProperty("hadoop.tmp.dir"));
  }

  @Test
  public void testReload() throws Throwable {
    SubConf conf = new SubConf(true);
    assertFalse(conf.isReloaded());
    Configuration.addDefaultResource(EMPTY_CONFIGURATION_XML);
    assertTrue(conf.isReloaded());
    Properties properties = conf.getProperties();
  }

  @Test
  public void testReloadNotQuiet() throws Throwable {
    SubConf conf = new SubConf(true);
    conf.setQuietMode(false);
    assertFalse(conf.isReloaded());
    conf.addResource("not-a-valid-resource");
    assertTrue(conf.isReloaded());
    try {
      Properties properties = conf.getProperties();
      fail("Should not have got here");
    } catch (RuntimeException e) {
      assertTrue(e.toString(),e.getMessage().contains("not found"));
    }
  }

  private static class SubConf extends Configuration {

    private boolean reloaded;

    /**
     * A new configuration where the behavior of reading from the default resources
     * can be turned off.
     *
     * If the parameter {@code loadDefaults} is false, the new instance will not
     * load resources from the default files.
     *
     * @param loadDefaults specifies whether to load from the default files
     */
    private SubConf(boolean loadDefaults) {
      super(loadDefaults);
    }

    public Properties getProperties() {
      return super.getProps();
    }

    /**
     * {@inheritDoc}.
     * Sets the reloaded flag.
     */
    @Override
    public void reloadConfiguration() {
      super.reloadConfiguration();
      reloaded = true;
    }

    public boolean isReloaded() {
      return reloaded;
    }

    public void setReloaded(boolean reloaded) {
      this.reloaded = reloaded;
    }
  }

}
