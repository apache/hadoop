package org.apache.hadoop.net;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestCachedDNSToSwitchMapping {

  @Test
  public void testReloadCachedMappings() {
    StaticMapping.resetMap();
    StaticMapping.addNodeToRack("127.0.0.1", "/rack0");
    StaticMapping.addNodeToRack("notexisit.host.com", "/rack1");
    CachedDNSToSwitchMapping cacheMapping =
        new CachedDNSToSwitchMapping(new StaticMapping());
    List<String> names = new ArrayList<>();
    names.add("localhost");
    names.add("notexisit.host.com");
    cacheMapping.resolve(names);
    Assert.assertTrue(cacheMapping.getSwitchMap().containsKey("127.0.0.1"));
    Assert.assertTrue(cacheMapping.getSwitchMap().containsKey("notexisit.host.com"));
    cacheMapping.reloadCachedMappings(names);
    Assert.assertEquals(0, cacheMapping.getSwitchMap().keySet().size());
  }
}
