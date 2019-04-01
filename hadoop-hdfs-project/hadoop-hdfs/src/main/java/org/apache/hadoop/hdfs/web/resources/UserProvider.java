package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.glassfish.hk2.api.Factory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

@Provider
public class UserProvider implements Factory<UserGroupInformation> {

  @Context HttpServletRequest request;
  @Context ServletContext servletcontext;

  @Override
  public UserGroupInformation provide() {
    final Configuration conf = (Configuration) servletcontext
        .getAttribute(JspHelper.CURRENT_CONF);
    try {
      return JspHelper.getUGI(servletcontext, request, conf,
          UserGroupInformation.AuthenticationMethod.KERBEROS, false);
    } catch (IOException e) {
      throw new SecurityException(
          SecurityUtil.FAILED_TO_GET_UGI_MSG_HEADER + " " + e, e);
    }
  }

  @Override
  public void dispose(UserGroupInformation userGroupInformation) {
  }
}
