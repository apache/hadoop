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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.junit.jupiter.api.Test;

/**
 * A SignerSecretProvider written against the deprecated
 * {@link SignerSecretProvider#init(Properties, ServletContext, long)} must keep
 * working without being recompiled, because implementations of it live outside
 * this tree - signer.secret.provider takes a classname.
 * <p>
 * These tests pin that contract. They are the reason
 * {@link SignerSecretProvider#init(Properties, ServletContext, long)} is a
 * concrete bridge rather than a changed abstract method, and they go when it
 * does.
 */
@SuppressWarnings("deprecation")
public class TestSignerSecretProviderCompatibility {

  private static final byte[] SECRET =
      "secret".getBytes(StandardCharsets.UTF_8);

  /**
   * A provider as it would have been written before SecretProviderContext
   * existed: it overrides init and takes a ServletContext.
   */
  private static final class LegacyProvider extends SignerSecretProvider {
    private ServletContext seen;
    private boolean initCalled;

    @Override
    public void init(Properties config, ServletContext servletContext,
        long tokenValidity) {
      this.seen = servletContext;
      this.initCalled = true;
    }

    @Override
    public byte[] getCurrentSecret() {
      return SECRET;
    }

    @Override
    public byte[][] getAllSecrets() {
      return new byte[][]{SECRET};
    }
  }

  /**
   * The same, extending RolloverSignerSecretProvider and chaining to super as
   * such a provider is expected to.
   */
  private static final class LegacyRolloverProvider
      extends RolloverSignerSecretProvider {
    private boolean initCalled;

    @Override
    public void init(Properties config, ServletContext servletContext,
        long tokenValidity) throws Exception {
      this.initCalled = true;
      super.init(config, servletContext, tokenValidity);
    }

    @Override
    protected byte[] generateNewSecret() {
      return SECRET;
    }
  }

  /** A provider written against the servlet-free method. */
  private static final class ContextProvider extends SignerSecretProvider {
    private SecretProviderContext seen;

    @Override
    public void initialize(Properties config, SecretProviderContext context,
        long tokenValidity) {
      this.seen = context;
    }

    @Override
    public byte[] getCurrentSecret() {
      return SECRET;
    }

    @Override
    public byte[][] getAllSecrets() {
      return new byte[][]{SECRET};
    }
  }

  /** A provider that implements neither method. */
  private static final class UninitializableProvider extends SignerSecretProvider {
    @Override
    public byte[] getCurrentSecret() {
      return SECRET;
    }

    @Override
    public byte[][] getAllSecrets() {
      return new byte[][]{SECRET};
    }
  }

  @Test
  public void testLegacyProviderStillReceivesTheServletContext()
      throws Exception {
    ServletContext servletContext = mock(ServletContext.class);
    LegacyProvider provider = new LegacyProvider();

    provider.init(new Properties(), servletContext, 1000);

    assertTrue(provider.initCalled, "the legacy override should have run");
    assertSame(servletContext, provider.seen);
  }

  @Test
  public void testLegacyRolloverProviderIsStillRolledOver() throws Exception {
    LegacyRolloverProvider provider = new LegacyRolloverProvider();
    try {
      provider.init(new Properties(), mock(ServletContext.class), 100000);

      // The override ran, and chaining to super still reached
      // RolloverSignerSecretProvider.initialize, which seeds the secret. Were
      // the bridge dispatched statically, this would silently be null.
      assertTrue(provider.initCalled, "the legacy override should have run");
      assertArrayEquals(SECRET, provider.getCurrentSecret());
    } finally {
      provider.destroy();
    }
  }

  @Test
  public void testContextProviderIsReachedThroughTheDeprecatedEntryPoint()
      throws Exception {
    Map<String, Object> attributes = new HashMap<>();
    ServletContext servletContext = mock(ServletContext.class);
    when(servletContext.getAttribute("a"))
        .thenAnswer(invocation -> attributes.get("a"));

    ContextProvider provider = new ContextProvider();
    provider.init(new Properties(), servletContext, 1000);

    assertNotNull(provider.seen, "initialize should have been called");

    // Writes reach the real ServletContext, so an object a provider shares
    // this way is still found there by everything that looks for it.
    provider.seen.setAttribute("a", "value");
    org.mockito.Mockito.verify(servletContext).setAttribute("a", "value");
  }

  @Test
  public void testNullServletContextYieldsAUsableStore() throws Exception {
    ContextProvider provider = new ContextProvider();
    provider.init(new Properties(), null, 1000);

    assertNotNull(provider.seen,
        "a null ServletContext should still yield a store");
    provider.seen.setAttribute("a", "value");
    assertSame("value", provider.seen.getAttribute("a"));
    assertFalse(provider.seen instanceof ServletSecretProviderContext);
  }

  @Test
  public void testProviderImplementingNeitherMethodFailsLoudly() {
    UninitializableProvider provider = new UninitializableProvider();

    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class,
            () -> provider.init(new Properties(), null, 1000));
    assertTrue(e.getMessage().contains(UninitializableProvider.class.getName()));
  }
}
