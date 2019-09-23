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

package org.apache.hadoop.ozone.s3.header;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class tests Authorization header format v2.
 */
public class TestAuthorizationHeaderV2 {

  @Test
  public void testAuthHeaderV2() throws OS3Exception {
    try {
      String auth = "AWS accessKey:signature";
      AuthorizationHeaderV2 v2 = new AuthorizationHeaderV2(auth);
      assertEquals(v2.getAccessKeyID(), "accessKey");
      assertEquals(v2.getSignature(), "signature");
    } catch (OS3Exception ex) {
      fail("testAuthHeaderV2 failed");
    }
  }

  @Test
  public void testIncorrectHeader1() throws OS3Exception {
    try {
      String auth = "AAA accessKey:signature";
      new AuthorizationHeaderV2(auth);
      fail("testIncorrectHeader");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }

  @Test
  public void testIncorrectHeader2() throws OS3Exception {
    try {
      String auth = "AWS :accessKey";
      new AuthorizationHeaderV2(auth);
      fail("testIncorrectHeader");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }

  @Test
  public void testIncorrectHeader3() throws OS3Exception {
    try {
      String auth = "AWS :signature";
      new AuthorizationHeaderV2(auth);
      fail("testIncorrectHeader");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }

  @Test
  public void testIncorrectHeader4() throws OS3Exception {
    try {
      String auth = "AWS accessKey:";
      new AuthorizationHeaderV2(auth);
      fail("testIncorrectHeader");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }
}
