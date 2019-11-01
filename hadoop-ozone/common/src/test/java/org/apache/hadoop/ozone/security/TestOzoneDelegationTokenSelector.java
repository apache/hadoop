/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.security;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.apache.hadoop.ozone.security.OzoneTokenIdentifier.KIND_NAME;

/**
 * Class to test OzoneDelegationTokenSelector.
 */
public class TestOzoneDelegationTokenSelector {


  @Test
  public void testTokenSelector() {

    // set dummy details for identifier and password in token.
    byte[] identifier =
        RandomStringUtils.randomAlphabetic(10)
            .getBytes(StandardCharsets.UTF_8);
    byte[] password =
        RandomStringUtils.randomAlphabetic(10)
            .getBytes(StandardCharsets.UTF_8);

    Token<OzoneTokenIdentifier> tokenIdentifierToken =
        new Token<>(identifier, password, KIND_NAME, getService());

    OzoneDelegationTokenSelector ozoneDelegationTokenSelector =
        new OzoneDelegationTokenSelector();

    Text service = new Text("om1:9862");

    Token<OzoneTokenIdentifier> selectedToken =
        ozoneDelegationTokenSelector.selectToken(service,
            Collections.singletonList(tokenIdentifierToken));


    Assert.assertNotNull(selectedToken);


    tokenIdentifierToken.setService(new Text("om1:9863"));
    selectedToken =
        ozoneDelegationTokenSelector.selectToken(service,
            Collections.singletonList(tokenIdentifierToken));

    Assert.assertNull(selectedToken);

    service = new Text("om1:9863");
    selectedToken =
        ozoneDelegationTokenSelector.selectToken(service,
            Collections.singletonList(tokenIdentifierToken));

    Assert.assertNotNull(selectedToken);

  }


  private Text getService() {
    return new Text("om1:9862,om2:9862,om3:9862");
  }


}
