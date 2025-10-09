package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.glassfish.jersey.CommonProperties;

public class JsonProviderFeature implements Feature {
  @Override
  public boolean configure(FeatureContext context) {
    //Auto discovery should be disabled to ensure the custom providers will be used
    context.property(CommonProperties.MOXY_JSON_FEATURE_DISABLE, true);
    context.register(IncludeRootJSONProvider.class, 2001);
    context.register(ExcludeRootJSONProvider.class, 2002);
    return true;
  }
}
