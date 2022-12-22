package org.apache.hadoop.fs.azurebfs.utils;

import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;

/**
 * Utility class to provide method which can return if the account is namespace
 * enabled or not.
 * */
public class NamespaceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(NamespaceUtil.class);

  private NamespaceUtil() {

  }

  /**
   * Return if the account used in the provided abfsClient object namespace enabled
   * or not.
   * It would call {@link org.apache.hadoop.fs.azurebfs.services.AbfsClient#getAclStatus(String, TracingContext)}.
   * <ol>
   *   <li>
   *     If the API call is successful, then the account is namespace enabled.
   *   </li>
   *   <li>
   *     If the server returns with {@link java.net.HttpURLConnection#HTTP_BAD_REQUEST}, the account is non-namespace enabled.
   *   </li>
   *   <li>
   *     If the server call gets some other exception, then the method would throw the exception.
   *   </li>
   * </ol>
   * */
  public static Boolean isNamespaceEnabled(final AbfsClient abfsClient,
      final TracingContext tracingContext)
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
