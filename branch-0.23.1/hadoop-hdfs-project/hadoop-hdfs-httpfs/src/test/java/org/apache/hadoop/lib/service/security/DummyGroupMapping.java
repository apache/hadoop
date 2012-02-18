package org.apache.hadoop.lib.service.security;

import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DummyGroupMapping implements GroupMappingServiceProvider {

  @Override
  @SuppressWarnings("unchecked")
  public List<String> getGroups(String user) throws IOException {
    if (user.equals("root")) {
      return Arrays.asList("admin");
    }
    else if (user.equals("nobody")) {
      return Arrays.asList("nobody");
    } else {
      String[] groups = HadoopUsersConfTestHelper.getHadoopUserGroups(user);
      return (groups != null) ? Arrays.asList(groups) : Collections.EMPTY_LIST;
    }
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
  }
}
