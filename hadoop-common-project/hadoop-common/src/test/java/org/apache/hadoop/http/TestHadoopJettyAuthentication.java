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
package org.apache.hadoop.http;

import java.security.Principal;
import javax.security.auth.Subject;

import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHadoopJettyAuthentication {

  @Test
  public void testBuildAuthenticationProducesUserWithPrincipalName() {
    Authentication.User auth =
        HadoopJettyAuthentication.buildAuthentication("alice");

    assertNotNull(auth, "buildAuthentication should return a non-null User");
    assertEquals("HADOOP", auth.getAuthMethod());

    UserIdentity identity = auth.getUserIdentity();
    assertNotNull(identity, "UserIdentity should not be null");

    Principal principal = identity.getUserPrincipal();
    assertNotNull(principal, "Principal should not be null");
    assertEquals("alice", principal.getName());
    assertEquals("alice", principal.toString(),
        "Principal toString should mirror the name");
  }

  @Test
  public void testBuildAuthenticationSubjectContainsPrincipal() {
    Authentication.User auth =
        HadoopJettyAuthentication.buildAuthentication("bob");

    Subject subject = auth.getUserIdentity().getSubject();
    assertNotNull(subject, "Subject should not be null");
    assertTrue(subject.isReadOnly(),
        "Subject should be read-only to prevent post-attach mutation");
    assertEquals(1, subject.getPrincipals().size());
    assertEquals("bob",
        subject.getPrincipals().iterator().next().getName());
  }

  @Test
  public void testBuildAuthenticationUserIsNotInAnyRole() {
    // We do not propagate roles from Hadoop; the helper must not pretend
    // the user has any.
    Authentication.User auth =
        HadoopJettyAuthentication.buildAuthentication("carol");

    assertFalse(auth.isUserInRole(null, "admin"));
    assertFalse(auth.getUserIdentity().isUserInRole("admin", null));
  }

  @Test
  public void testLogoutIsNoOp() {
    // Programmatic logout is not supported. Both overloads must be
    // callable without throwing; an unexpected exception fails the test.
    Authentication.User auth =
        HadoopJettyAuthentication.buildAuthentication("dave");

    auth.logout();
    assertNull(auth.logout(null),
        "logout(ServletRequest) returns null (no logout support)");
  }

  @Test
  public void testAttachIsNullSafe() {
    // attach() is called unconditionally by auth filters; null args and
    // requests that are not Jetty Requests must be silently skipped.
    HadoopJettyAuthentication.attach(null, "alice");
    HadoopJettyAuthentication.attach(null, null);
  }
}
