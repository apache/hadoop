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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


public class ITestAbfsCustomTimeout extends AbstractAbfsIntegrationTest {

    private boolean optimizeTimeout;
    private int maxRequestTimeout;
    private int requestTimeoutIncRate;
    private HashMap<AbfsRestOperationType, Integer> opMap = new HashMap<AbfsRestOperationType, Integer>();

    public ITestAbfsCustomTimeout() throws Exception {
        super();
        initOpTypeRequestTimeout();
    }

    @Test
    public void testOptimizer() throws IOException, IllegalAccessException {

        AbfsConfiguration abfsConfig = getConfiguration();
        abfsConfig.set(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS, "true");
        abfsConfig.set(ConfigurationKeys.AZURE_MAX_REQUEST_TIMEOUT, "90");
        abfsConfig.set(ConfigurationKeys.AZURE_REQUEST_TIMEOUT_INCREASE_RATE, "2");
        optimizeTimeout =true;
        maxRequestTimeout = 90;
        requestTimeoutIncRate = 2;
        AbfsConfiguration newConfig = new AbfsConfiguration(abfsConfig.getRawConfiguration(), getAccountName());

        for (Map.Entry<AbfsRestOperationType, Integer> it : opMap.entrySet()) {
            AbfsRestOperationType opType = it.getKey();
            int timeout = it.getValue();
            String config = "";
            if (opType == AbfsRestOperationType.CreateFileSystem) {
                config = ConfigurationKeys.AZURE_CREATE_FS_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.GetFileSystemProperties) {
                config = ConfigurationKeys.AZURE_GET_FS_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.SetFileSystemProperties) {
                config = ConfigurationKeys.AZURE_SET_FS_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.DeleteFileSystem) {
                config = ConfigurationKeys.AZURE_DELETE_FS_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.ListPaths) {
                config = ConfigurationKeys.AZURE_LIST_PATH_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.CreatePath) {
                config = ConfigurationKeys.AZURE_CREATE_PATH_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.RenamePath) {
                config = ConfigurationKeys.AZURE_RENAME_PATH_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.GetAcl) {
                config = ConfigurationKeys.AZURE_GET_ACL_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.GetPathProperties) {
                config = ConfigurationKeys.AZURE_GET_PATH_PROPERTIES_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.SetPathProperties) {
                config = ConfigurationKeys.AZURE_SET_PATH_PROPERTIES_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.SetAcl) {
                config = ConfigurationKeys.AZURE_SET_ACL_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.SetOwner) {
                config = ConfigurationKeys.AZURE_SET_OWNER_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.SetPermissions) {
                config = ConfigurationKeys.AZURE_SET_PERMISSIONS_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.CheckAccess) {
                config = ConfigurationKeys.AZURE_CHECK_ACCESS_REQUEST_TIMEOUT;
            }
            else if (opType == AbfsRestOperationType.GetPathStatus) {
                config = ConfigurationKeys.AZURE_GET_PATH_STATUS_REQUEST_TIMEOUT;
            }
            abfsConfig.set(config, Integer.toString(timeout));
            testInitTimeoutOptimizer(opType, 3, timeout, newConfig);
            abfsConfig.unset(config);
        }

        abfsConfig.set(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS, "false");

    }

    public void testInitTimeoutOptimizer(AbfsRestOperationType opType, int maxRetryCount, int expectedReqTimeout, AbfsConfiguration abfsConfig) throws IOException {

        AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(abfsConfig.getRawConfiguration());
        AbfsClient client = fs.getAbfsStore().getClient();
        String query = client.createDefaultUriQueryBuilder().toString();
        URL url = client.createRequestUrl("/testPath", query);
        TimeoutOptimizer opt = new TimeoutOptimizer(url, opType, client.getRetryPolicy(), getConfiguration());
        int retryCount = 0;
        while (retryCount <= maxRetryCount) {
            assertEquals(expectedReqTimeout, opt.getRequestTimeout());
            assertEquals(expectedReqTimeout, opt.getReadTimeout());
            assertEquals(expectedReqTimeout-1, opt.getConnTimeout());
            retryCount += 1;
            if (expectedReqTimeout * requestTimeoutIncRate > maxRequestTimeout) {
                expectedReqTimeout = maxRequestTimeout;
            } else {
                expectedReqTimeout *= requestTimeoutIncRate;
            }
            opt.updateRetryTimeout(retryCount);
        }

    }

    private void initOpTypeRequestTimeout() {
        opMap.put(AbfsRestOperationType.GetFileSystemProperties, new Integer(5));
        opMap.put(AbfsRestOperationType.SetFileSystemProperties, new Integer(5));
        opMap.put(AbfsRestOperationType.DeleteFileSystem, new Integer(5));
        opMap.put(AbfsRestOperationType.ListPaths, new Integer(10));
        opMap.put(AbfsRestOperationType.CreatePath, new Integer(5));
        opMap.put(AbfsRestOperationType.RenamePath, new Integer(5));
        opMap.put(AbfsRestOperationType.GetAcl, new Integer(5));
        opMap.put(AbfsRestOperationType.SetAcl, new Integer(5));
        opMap.put(AbfsRestOperationType.GetPathProperties, new Integer(5));
        opMap.put(AbfsRestOperationType.GetPathStatus, new Integer(5));
        opMap.put(AbfsRestOperationType.SetOwner, new Integer(5));
        opMap.put(AbfsRestOperationType.SetPermissions, new Integer(5));
        opMap.put(AbfsRestOperationType.SetPathProperties, new Integer(5));
        opMap.put(AbfsRestOperationType.CheckAccess, new Integer(5));
    }
}
