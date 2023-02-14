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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.http.client.utils.URIBuilder;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_TIMEOUT;

public class TimeoutOptimizer {
    AbfsConfiguration abfsConfiguration;
    private URL url;
    private AbfsRestOperationType opType;
    private ExponentialRetryPolicy retryPolicy;
    private int requestTimeout;
    private int readTimeout = -1;
    private int connTimeout = -1;
    private int maxReqTimeout;
    private int timeoutIncRate;
    private boolean shouldOptimizeTimeout;

    public TimeoutOptimizer(URL url, AbfsRestOperationType opType, ExponentialRetryPolicy retryPolicy, AbfsConfiguration abfsConfiguration) {
        this.url = url;
        this.opType = opType;
        if (opType != null) {
            this.retryPolicy = retryPolicy;
            this.abfsConfiguration = abfsConfiguration;
            if (abfsConfiguration.get(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS) == null) {
                this.shouldOptimizeTimeout = false;
            }
            else {
                this.shouldOptimizeTimeout = Boolean.parseBoolean(abfsConfiguration.get(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS));
            }
            if (this.shouldOptimizeTimeout) {
                this.maxReqTimeout = Integer.parseInt(abfsConfiguration.get(ConfigurationKeys.AZURE_MAX_REQUEST_TIMEOUT));
                this.timeoutIncRate = Integer.parseInt(abfsConfiguration.get(ConfigurationKeys.AZURE_REQUEST_TIMEOUT_INCREASE_RATE));
                initTimeouts();
                updateUrl();
            }

        } else {
            this.shouldOptimizeTimeout = false;
        }
    }

    public void updateRetryTimeout(int retryCount) {
        if (!this.shouldOptimizeTimeout) {
            return;
        }

        // update all timeout values
        updateTimeouts(retryCount);
        updateUrl();
    }

    public URL getUrl() {
        return url;
    }
    public boolean getShouldOptimizeTimeout() { return this.shouldOptimizeTimeout; }

    public int getRequestTimeout() { return requestTimeout; }

    public int getReadTimeout() {
        return readTimeout;
    }

    public int getReadTimeout(final int defaultTimeout) {
        if (readTimeout != -1 && shouldOptimizeTimeout) {
            return readTimeout;
        }
        return defaultTimeout;
    }

    public int getConnTimeout() {
        return connTimeout;
    }

    public int getConnTimeout(final int defaultTimeout) {
        if (connTimeout == -1) {
            return defaultTimeout;
        }
        return connTimeout;
    }

    private void initTimeouts() {
        if (!shouldOptimizeTimeout) {
            requestTimeout = -1;
            readTimeout = -1;
            connTimeout = -1;
            return;
        }

        String query = url.getQuery();
        int timeoutPos = query.indexOf("timeout");
        if (timeoutPos < 0) {
            // no value of timeout exists in the URL
            // no optimization is needed for this particular request as well
            requestTimeout = -1;
            readTimeout = -1;
            connTimeout = -1;
            shouldOptimizeTimeout = false;
            return;
        }

        String timeout = "";
        if (opType == AbfsRestOperationType.CreateFileSystem) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_CREATE_FS_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.GetFileSystemProperties) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_FS_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.SetFileSystemProperties) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_FS_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.DeleteFileSystem) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_DELETE_FS_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.ListPaths) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_LIST_PATH_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.CreatePath) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_CREATE_PATH_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.RenamePath) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_RENAME_PATH_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.GetAcl) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_ACL_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.GetPathProperties) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_PATH_PROPERTIES_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.SetPathProperties) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_PATH_PROPERTIES_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.SetAcl) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_ACL_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.SetOwner) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_OWNER_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.SetPermissions) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_PERMISSIONS_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.Append) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_APPEND_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.CheckAccess) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_CHECK_ACCESS_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.GetPathStatus) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_PATH_STATUS_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.Flush) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_FLUSH_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.ReadFile) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_READFILE_REQUEST_TIMEOUT);
        }
        else if (opType == AbfsRestOperationType.LeasePath) {
            timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_LEASE_PATH_REQUEST_TIMEOUT);
        }
        if (timeout == null) {
            timeout = DEFAULT_TIMEOUT;
        }
        requestTimeout = Integer.parseInt(timeout);
        readTimeout = requestTimeout;
        connTimeout = requestTimeout - 1;
        updateUrl();
    }

    private void updateTimeouts(int retryCount) {
        if (retryCount == 0) {
            return;
        }
        int maxRetryCount = retryPolicy.getRetryCount();
        if (retryCount <= maxRetryCount && timeoutIncRate > 0) {
            // retry count is still valid
            // timeout increment rate is a valid value
            if ((requestTimeout * timeoutIncRate) > maxReqTimeout) {
                requestTimeout = maxReqTimeout;
            } else {
                requestTimeout *= timeoutIncRate;
            }
            readTimeout = requestTimeout;
            connTimeout = requestTimeout - 1;
        }
    }

    private void updateUrl() {
        // updates URL with existing request timeout value
        URL updatedUrl = null;
        try {
            URIBuilder uriBuilder = new URIBuilder(url.toURI());
            uriBuilder.setParameter(HttpQueryParams.QUERY_PARAM_TIMEOUT, Integer.toString(requestTimeout));
            updatedUrl = uriBuilder.build().toURL();
        } catch (URISyntaxException e) {

        } catch (MalformedURLException e) {

        }
        url = updatedUrl;
    }

}
