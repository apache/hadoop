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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.Test;

public class TestAuthToken {

  @Test
  public void testConstructor() throws Exception {
    try {
      new AuthToken(null, "p", "t");
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
    try {
      new AuthToken("", "p", "t");
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
    try {
      new AuthToken("u", null, "t");
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
    try {
      new AuthToken("u", "", "t");
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
    try {
      new AuthToken("u", "p", null);
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
    try {
      new AuthToken("u", "p", "");
      fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      fail();
    }
    new AuthToken("u", "p", "t");
  }

  @Test
  public void testGetters() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthToken token = new AuthToken("u", "p", "t");
    token.setExpires(expires);
    assertEquals("u", token.getUserName());
    assertEquals("p", token.getName());
    assertEquals("t", token.getType());
    assertEquals(expires, token.getExpires());
    assertFalse(token.isExpired());
    Thread.sleep(70);               // +20 msec fuzz for timer granularity.
    assertTrue(token.isExpired());
  }

  @Test
  public void testToStringAndParse() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthToken token = new AuthToken("u", "p", "t");
    token.setExpires(expires);
    String str = token.toString();
    token = AuthToken.parse(str);
    assertEquals("p", token.getName());
    assertEquals("t", token.getType());
    assertEquals(expires, token.getExpires());
    assertFalse(token.isExpired());
    Thread.sleep(70);               // +20 msec fuzz for timer granularity.
    assertTrue(token.isExpired());
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
      fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      fail();
    }
  }
}
