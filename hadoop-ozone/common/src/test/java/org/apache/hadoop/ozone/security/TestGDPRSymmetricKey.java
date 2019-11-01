/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.Assert;
import org.junit.Test;

import java.security.SecureRandom;

/**
 * Tests GDPRSymmetricKey structure.
 */
public class TestGDPRSymmetricKey {

  @Test
  public void testKeyGenerationWithDefaults() throws Exception {
    GDPRSymmetricKey gkey = new GDPRSymmetricKey(new SecureRandom());

    Assert.assertTrue(gkey.getCipher().getAlgorithm()
        .equalsIgnoreCase(OzoneConsts.GDPR_ALGORITHM_NAME));

    gkey.getKeyDetails().forEach(
        (k, v) -> Assert.assertTrue(v.length() > 0));
  }

  @Test
  public void testKeyGenerationWithValidInput() throws Exception {
    GDPRSymmetricKey gkey = new GDPRSymmetricKey(
        RandomStringUtils.randomAlphabetic(16),
        OzoneConsts.GDPR_ALGORITHM_NAME);

    Assert.assertTrue(gkey.getCipher().getAlgorithm()
        .equalsIgnoreCase(OzoneConsts.GDPR_ALGORITHM_NAME));

    gkey.getKeyDetails().forEach(
        (k, v) -> Assert.assertTrue(v.length() > 0));
  }

  @Test
  public void testKeyGenerationWithInvalidInput() throws Exception {
    GDPRSymmetricKey gkey = null;
    try{
      gkey = new GDPRSymmetricKey(RandomStringUtils.randomAlphabetic(5),
          OzoneConsts.GDPR_ALGORITHM_NAME);
    } catch (IllegalArgumentException ex) {
      Assert.assertTrue(ex.getMessage()
          .equalsIgnoreCase("Secret must be exactly 16 characters"));
      Assert.assertTrue(gkey == null);
    }
  }


}
