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

package org.apache.hadoop.util;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test MR resource with visibility settings.
 */
public class TestResourceWithVisibilitySetting {
  @Test
  public void testSerialize() {
    String path0 = "path0";
    String visSettings0 = "public";
    ResourceWithVisibilitySetting item0 =
        new ResourceWithVisibilitySetting(path0, visSettings0);
    assertTrue("Serialized string should be" + path0
            + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
            + visSettings0 + ", but turn out be "
            + ResourceWithVisibilitySetting.serialize(item0),
        ResourceWithVisibilitySetting.serialize(item0).
            equals(path0
                + ResourceWithVisibilitySetting.PATH_SETTING_DELIMITER
                + visSettings0));

    String path1 = "path1";
    String visSettings1 = "";
    ResourceWithVisibilitySetting item1 =
        new ResourceWithVisibilitySetting(path1, visSettings1);
    assertTrue("Serialized string should be " + path1 + ", but is:"
            + ResourceWithVisibilitySetting.serialize(item1),
        ResourceWithVisibilitySetting.serialize(item1).equals(path1));

    String path2 = "path2";
    ResourceWithVisibilitySetting item2 =
        new ResourceWithVisibilitySetting(path2, null);
    assertTrue("Serialized string should be " + path2 + ", but is:"
            + ResourceWithVisibilitySetting.serialize(item2),
        ResourceWithVisibilitySetting.serialize(item2).equals(path2));
  }

  @Test
  public void testDeserialize() {
    String path0 = "hdfs://nn/mr-tmp";
    ResourceWithVisibilitySetting item0 =
        ResourceWithVisibilitySetting.deserialize(path0);
    assertTrue("Path should " + path0
            + ", but is " + item0.getPathStr(),
        item0.getPathStr().equals(path0));
    assertNull("Settings is not null",
        item0.getVisibilitySettings());

    String path1 = "hdfs://nn/mr-tmp::public";
    ResourceWithVisibilitySetting item1 =
        ResourceWithVisibilitySetting.deserialize(path1);
    assertTrue("Path should be hdfs://nn/mr-tmp, but is "
            + item1.getPathStr(),
        item1.getPathStr().equals("hdfs://nn/mr-tmp"));
    assertTrue("Setting should be public",
        item1.getVisibilitySettings().equals("public"));

    String path2 = "file:///file1::application";
    ResourceWithVisibilitySetting item2 =
        ResourceWithVisibilitySetting.deserialize(path2);
    assertTrue("Path should be file:///file1, but is "
            + item2.getPathStr(),
        item2.getPathStr().equals("file:///file1"));
    assertTrue("Setting should be application",
        item2.getVisibilitySettings().equals("application"));
  }
}
