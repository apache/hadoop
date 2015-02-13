/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.util;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.Assert;
import org.junit.Test;

public class TestAuthToken {

  @Test
  public void testConstructor() throws Exception {
    try {
      new AuthToken(null, "p", "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthToken("", "p", "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthToken("u", null, "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthToken("u", "", "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthToken("u", "p", null);
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthToken("u", "p", "");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    new AuthToken("u", "p", "t");
  }

  @Test
  public void testGetters() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthToken token = new AuthToken("u", "p", "t");
    token.setExpires(expires);
    Assert.assertEquals("u", token.getUserName());
    Assert.assertEquals("p", token.getName());
    Assert.assertEquals("t", token.getType());
    Assert.assertEquals(expires, token.getExpires());
    Assert.assertFalse(token.isExpired());
    Thread.sleep(70);               // +20 msec fuzz for timer granularity.
    Assert.assertTrue(token.isExpired());
  }

  @Test
  public void testToStringAndParse() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthToken token = new AuthToken("u", "p", "t");
    token.setExpires(expires);
    String str = token.toString();
    token = AuthToken.parse(str);
    Assert.assertEquals("p", token.getName());
    Assert.assertEquals("t", token.getType());
    Assert.assertEquals(expires, token.getExpires());
    Assert.assertFalse(token.isExpired());
    Thread.sleep(70);               // +20 msec fuzz for timer granularity.
    Assert.assertTrue(token.isExpired());
  }

  @Test
  public void testParseValidAndInvalid() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthToken token = new AuthToken("u", "p", "t");
    token.setExpires(expires);
    String ostr = token.toString();

    String str1 = "\"" + ostr + "\"";
    AuthToken.parse(str1);
    
    String str2 = ostr + "&s=1234";
    AuthToken.parse(str2);

    String str = ostr.substring(0, ostr.indexOf("e="));
    try {
      AuthToken.parse(str);
      Assert.fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      Assert.fail();
    }
  }
}
