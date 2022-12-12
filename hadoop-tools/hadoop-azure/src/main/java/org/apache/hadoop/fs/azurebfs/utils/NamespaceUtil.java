package org.apache.hadoop.fs.azurebfs.utils;

import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;

public class NamespaceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(NamespaceUtil.class);

  private NamespaceUtil() {

  }

  public static Boolean isNamespaceEnabled(final AbfsClient abfsClient, final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    Boolean isNamespaceEnabled;
    try {
      LOG.debug("Get root ACL status");
      abfsClient.getAclStatus(AbfsHttpConstants.ROOT_PATH, tracingContext);
      isNamespaceEnabled = true;
    } catch (AbfsRestOperationException ex) {
      // Get ACL status is a HEAD request, its response doesn't contain
      // errorCode
      // So can only rely on its status code to determine its account type.
      if (HttpURLConnection.HTTP_BAD_REQUEST != ex.getStatusCode()) {
        throw ex;
      }
      isNamespaceEnabled = false;
    } catch (AzureBlobFileSystemException ex) {
      throw ex;
    }
    return isNamespaceEnabled;
  }
}
