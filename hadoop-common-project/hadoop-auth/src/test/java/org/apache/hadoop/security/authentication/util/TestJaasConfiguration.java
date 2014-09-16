/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.util;

import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import org.junit.Assert;
import org.junit.Test;

public class TestJaasConfiguration {

  // We won't test actually using it to authenticate because that gets messy and
  // may conflict with other tests; but we can test that it otherwise behaves
  // correctly
  @Test
  public void test() throws Exception {
    String krb5LoginModuleName;
    if (System.getProperty("java.vendor").contains("IBM")) {
      krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
    } else {
      krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
    }

    ZKSignerSecretProvider.JaasConfiguration jConf =
            new ZKSignerSecretProvider.JaasConfiguration("foo", "foo/localhost",
            "/some/location/foo.keytab");
    AppConfigurationEntry[] entries = jConf.getAppConfigurationEntry("bar");
    Assert.assertNull(entries);
    entries = jConf.getAppConfigurationEntry("foo");
    Assert.assertEquals(1, entries.length);
    AppConfigurationEntry entry = entries[0];
    Assert.assertEquals(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            entry.getControlFlag());
    Assert.assertEquals(krb5LoginModuleName, entry.getLoginModuleName());
    Map<String, ?> options = entry.getOptions();
    Assert.assertEquals("/some/location/foo.keytab", options.get("keyTab"));
    Assert.assertEquals("foo/localhost", options.get("principal"));
    Assert.assertEquals("true", options.get("useKeyTab"));
    Assert.assertEquals("true", options.get("storeKey"));
    Assert.assertEquals("false", options.get("useTicketCache"));
    Assert.assertEquals("true", options.get("refreshKrb5Config"));
    Assert.assertEquals(6, options.size());
  }
}
