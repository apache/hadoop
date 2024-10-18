/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageMetadata;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageTagToManifestV2Plugin;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_META_NAMESPACE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_META_NAMESPACE;

/**
 * This class tests the ImageMetadata class for runc image metadata.
 */
public class TestImageMetadata {
  private Configuration conf;
  private ImageTagToManifestV2Plugin plugin;

  @Before
  public void setup() {
    conf = new Configuration();
    plugin = new ImageTagToManifestV2Plugin();
  }

  @Test
  public void testGetImageMetadataDefaultMetaNamespace() {
    String imageTag = "busybox:1.0.0";
    plugin.init(conf);
    String metaNamespace = conf.get(NM_RUNC_IMAGE_META_NAMESPACE,
        DEFAULT_NM_RUNC_IMAGE_META_NAMESPACE);
    ImageMetadata imageMetadata = new ImageMetadata(imageTag, metaNamespace);

    Assert.assertEquals("busybox:1.0.0", imageMetadata.getNameTag());
    Assert.assertEquals("library", imageMetadata.getMetaNamespace());
  }

  @Test
  public void testGetImageMetadataConfMetaNamespace() {
    String imageTag = "busybox:1.0.0";
    conf.set(NM_RUNC_IMAGE_META_NAMESPACE, "default");
    plugin.init(conf);
    String metaNamespace = conf.get(NM_RUNC_IMAGE_META_NAMESPACE,
        DEFAULT_NM_RUNC_IMAGE_META_NAMESPACE);
    ImageMetadata imageMetadata = new ImageMetadata(imageTag, metaNamespace);

    Assert.assertEquals("busybox:1.0.0", imageMetadata.getNameTag());
    Assert.assertEquals("default", imageMetadata.getMetaNamespace());
  }

  @Test
  public void testGetImageMetadataProvidedMetaNamespace() {
    String imageTag = "hadoop/busybox:1.0.0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "hadoop");

    Assert.assertEquals("busybox:1.0.0", imageMetadata.getNameTag());
    Assert.assertEquals("hadoop", imageMetadata.getMetaNamespace());
  }

  @Test
  public void testGetImageMetadataDefaultsLatestTag() {
    String imageTag = "busybox";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");

    Assert.assertEquals("busybox:latest", imageMetadata.getNameTag());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidImageTagFails() {
    String imageTag = "a/test/b";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidMetaNamespaceFails() {
    String imageTag = "busybox:1.0.0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "f@kespace");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidMetaNamespace2Fails() {
    String imageTag = "busybox:1.0.0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "$platform");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidMetaNamespace3Fails() {
    String imageTag = "busybox:1.0.0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "default#1");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidTagFails() {
    String imageTag = "busybox:1.0.0:0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataNamespaceInvalidTagFails() {
    String imageTag = "busybox:1.0.0:0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "hadoop");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidTag2Fails() {
    String imageTag = "busybox:#1.0.0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidTag3Fails() {
    String imageTag = "busybox:1.0$.0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidTag4Fails() {
    String imageTag = "//test:latest";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidTag5Fails() {
    String imageTag = "/:hadoop:latest";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidTag6Fails() {
    String imageTag = "a/b/:hadoop:latest";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataInvalidTag7Fails() {
    String imageTag = "hadoop::1,0";
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataNullImageTagFails() {
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata(null, "library");
    imageMetadata.getNameTag();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetImageMetadataEmptyImageTagFails() {
    plugin.init(conf);
    ImageMetadata imageMetadata =
        new ImageMetadata("", "library");
    imageMetadata.getNameTag();
  }
}
