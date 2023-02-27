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
import org.apache.hadoop.util.Preconditions;
import org.apache.http.client.utils.URIBuilder;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_TIMEOUT;

/**
 * Class handling whether timeout values should be optimized.
 * Timeout values optimized per request level,
 * based on configs in the settings.
 */
public class TimeoutOptimizer {
    private AbfsConfiguration abfsConfiguration;
    private URL url;
    private AbfsRestOperationType opType;
    private ExponentialRetryPolicy retryPolicy;
    private int requestTimeout;
    private int readTimeout = -1;
    private int maxReqTimeout = -1;
    private int timeoutIncRate = -1;
    private boolean shouldOptimizeTimeout;

    /**
     * Constructor to initialize the parameters in class,
     * depending upon what is configured in the settings.
     * @param url request URL
     * @param opType operation type
     * @param retryPolicy retry policy set for this instance of AbfsClient
     * @param abfsConfiguration current configuration
     */
    public TimeoutOptimizer(URL url, AbfsRestOperationType opType, ExponentialRetryPolicy retryPolicy, AbfsConfiguration abfsConfiguration) {
        this.url = url;
        this.opType = opType;
        if (opType != null) {
            this.retryPolicy = retryPolicy;
            this.abfsConfiguration = abfsConfiguration;
            String shouldOptimize = abfsConfiguration.get(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS);
            if (shouldOptimize == null || shouldOptimize.isEmpty()) {
                // config is not set
                this.shouldOptimizeTimeout = false;
            }
            else {
                this.shouldOptimizeTimeout = Boolean.parseBoolean(shouldOptimize);
                if (this.shouldOptimizeTimeout) {
                    // config is set to true
                    if (abfsConfiguration.get(ConfigurationKeys.AZURE_MAX_REQUEST_TIMEOUT) != null) {
                        this.maxReqTimeout = Integer.parseInt(abfsConfiguration.get(ConfigurationKeys.AZURE_MAX_REQUEST_TIMEOUT));
                    }
                    if (abfsConfiguration.get(ConfigurationKeys.AZURE_REQUEST_TIMEOUT_INCREASE_RATE) != null) {
                        this.timeoutIncRate = Integer.parseInt(abfsConfiguration.get(ConfigurationKeys.AZURE_REQUEST_TIMEOUT_INCREASE_RATE));
                    }
                    if (this.maxReqTimeout == -1 || this.timeoutIncRate == -1) {
                        this.shouldOptimizeTimeout = false;
                    } else {
                        initTimeouts();
                        updateUrl();
                    }
                }
            }
        } else {
            // optimization not required for opType == null
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
    public boolean getShouldOptimizeTimeout() {
        return this.shouldOptimizeTimeout;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public int getReadTimeout(final int defaultTimeout) {
        if (readTimeout != -1 && shouldOptimizeTimeout) {
            return readTimeout;
        }
        return defaultTimeout;
    }

    private void initTimeouts() {
        String query = url.getQuery();
        Integer timeoutPos = new Integer(query.indexOf("timeout"));
        if (timeoutPos != null && timeoutPos < 0) {
            // no value of timeout exists in the URL
            // no optimization is needed for this particular request as well
            shouldOptimizeTimeout = false;
            return;
        }

        String timeout = "";
        switch(opType) {
            case CreateFileSystem:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_CREATE_FS_REQUEST_TIMEOUT);
                break;
            case GetFileSystemProperties:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_FS_REQUEST_TIMEOUT);
                break;
            case SetFileSystemProperties:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_FS_REQUEST_TIMEOUT);
                break;
            case DeleteFileSystem:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_DELETE_FS_REQUEST_TIMEOUT);
                break;
            case ListPaths:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_LIST_PATH_REQUEST_TIMEOUT);
                break;
            case CreatePath:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_CREATE_PATH_REQUEST_TIMEOUT);
                break;
            case RenamePath:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_RENAME_PATH_REQUEST_TIMEOUT);
                break;
            case GetAcl:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_ACL_REQUEST_TIMEOUT);
                break;
            case GetPathProperties:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_PATH_PROPERTIES_REQUEST_TIMEOUT);
                break;
            case SetPathProperties:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_PATH_PROPERTIES_REQUEST_TIMEOUT);
                break;
            case SetAcl:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_ACL_REQUEST_TIMEOUT);
                break;
            case SetOwner:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_OWNER_REQUEST_TIMEOUT);
                break;
            case SetPermissions:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_SET_PERMISSIONS_REQUEST_TIMEOUT);
                break;
            case Append:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_APPEND_REQUEST_TIMEOUT);
                break;
            case CheckAccess:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_CHECK_ACCESS_REQUEST_TIMEOUT);
                break;
            case GetPathStatus:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_GET_PATH_STATUS_REQUEST_TIMEOUT);
                break;
            case Flush:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_FLUSH_REQUEST_TIMEOUT);
                break;
            case ReadFile:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_READFILE_REQUEST_TIMEOUT);
                break;
            case LeasePath:
                timeout = abfsConfiguration.get(ConfigurationKeys.AZURE_LEASE_PATH_REQUEST_TIMEOUT);
                break;
            default:
                timeout = DEFAULT_TIMEOUT;
        }
        if (timeout == null || timeout.isEmpty()) {
            // if any of the timeout values are not set
            // despite optimize config set to true
            timeout = DEFAULT_TIMEOUT;
        }
        requestTimeout = Integer.parseInt(timeout);
        Preconditions.checkArgument((requestTimeout <= maxReqTimeout),
                "Value of request timeout cannot be greater than the maximum allowed value.");
        readTimeout = requestTimeout * 1000;
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
            readTimeout = requestTimeout * 1000;
        }
    }

    private void updateUrl() {
        // updates URL with existing request timeout value
        if (!shouldOptimizeTimeout) {
            return;
        }
        URL updatedUrl = null;
        try {
            URIBuilder uriBuilder = new URIBuilder(url.toURI());
            uriBuilder.setParameter(HttpQueryParams.QUERY_PARAM_TIMEOUT, Integer.toString(requestTimeout));
            updatedUrl = uriBuilder.build().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        url = updatedUrl;
    }

}
