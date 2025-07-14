package org.apache.hadoop.security;

import org.junit.Assert;
import org.junit.Test;

public class TestAuthorizationContext {

  @Test
  public void testSetAndGetAuthorizationHeader() {
    byte[] header = "my-auth-header".getBytes();
    AuthorizationContext.setCurrentAuthorizationHeader(header);
    Assert.assertArrayEquals(header, AuthorizationContext.getCurrentAuthorizationHeader());
    AuthorizationContext.clear();
  }

  @Test
  public void testClearAuthorizationHeader() {
    byte[] header = "clear-me".getBytes();
    AuthorizationContext.setCurrentAuthorizationHeader(header);
    AuthorizationContext.clear();
    Assert.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
  }

  @Test
  public void testThreadLocalIsolation() throws Exception {
    byte[] mainHeader = "main-thread".getBytes();
    AuthorizationContext.setCurrentAuthorizationHeader(mainHeader);
    Thread t = new Thread(() -> {
      Assert.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
      byte[] threadHeader = "other-thread".getBytes();
      AuthorizationContext.setCurrentAuthorizationHeader(threadHeader);
      Assert.assertArrayEquals(threadHeader, AuthorizationContext.getCurrentAuthorizationHeader());
      AuthorizationContext.clear();
      Assert.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
    });
    t.start();
    t.join();
    // Main thread should still have its header
    Assert.assertArrayEquals(mainHeader, AuthorizationContext.getCurrentAuthorizationHeader());
    AuthorizationContext.clear();
  }

  @Test
  public void testNullAndEmptyHeader() {
    AuthorizationContext.setCurrentAuthorizationHeader(null);
    Assert.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
    byte[] empty = new byte[0];
    AuthorizationContext.setCurrentAuthorizationHeader(empty);
    Assert.assertArrayEquals(empty, AuthorizationContext.getCurrentAuthorizationHeader());
    AuthorizationContext.clear();
  }
}