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
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESOURCE;
import static org.mockito.ArgumentMatchers.nullable;


public class ITestAbfsCustomTimeout extends AbstractAbfsIntegrationTest {
    private int maxRequestTimeout;
    private int requestTimeoutIncRate;
    private HashMap<AbfsRestOperationType, Integer> opMap = new HashMap<AbfsRestOperationType, Integer>();
    private HashMap<AbfsRestOperationType, String> opTimeoutConfigMap = new HashMap<AbfsRestOperationType, String>();

    public ITestAbfsCustomTimeout() throws Exception {
        super();
        initOpTypeConfigs();
    }

    @Test
    public void testOptimizer() throws IOException, IllegalAccessException {

        AbfsConfiguration abfsConfig = getModifiedTestConfig();

        for (Map.Entry<AbfsRestOperationType, Integer> it : opMap.entrySet()) {
            AbfsRestOperationType opType = it.getKey();
            int timeout = it.getValue();
            String config = opTimeoutConfigMap.get(opType);
            abfsConfig.set(config, Integer.toString(timeout));
            testInitTimeoutOptimizer(opType, 3, timeout, abfsConfig);
            abfsConfig.unset(config);
        }

        abfsConfig.set(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS, "false");

    }

    /**
     * Test to verify working of timeout optimization with AbfsRestOperation execute calls
     * Currently tests only for a single API
     * @throws IOException
     * @throws IllegalAccessException
     */
    @Test
    public void testOptimizationInRestCall() throws IOException, IllegalAccessException {
        AbfsConfiguration abfsConfig = getModifiedTestConfig();
        AzureBlobFileSystem newFs = (AzureBlobFileSystem) FileSystem.newInstance(abfsConfig.getRawConfiguration());
        for (Map.Entry<AbfsRestOperationType, Integer> it : opMap.entrySet()) {
            AbfsRestOperationType opType = it.getKey();
            int timeout = it.getValue();
            String config = opTimeoutConfigMap.get(opType);
            abfsConfig.set(config, Integer.toString(timeout));
            AbfsRestOperation op = getMockAbfsRestOp(opType, newFs);
            final int[] finalTimeout = {timeout};
            Mockito.doAnswer(new Answer() {
                int requestCount = 4;

                public Object answer(InvocationOnMock invocation) {
                    if (requestCount > 0) {
                        requestCount--;
                        assertEquals(finalTimeout[0], op.getTimeoutOptimizer().getRequestTimeout());
                        if (finalTimeout[0] * requestTimeoutIncRate > maxRequestTimeout) {
                            finalTimeout[0] = maxRequestTimeout;
                        } else {
                            finalTimeout[0] *= requestTimeoutIncRate;
                        }
                    }
                    return op.getResult();
                }
            }).when(op).createHttpOperationInstance();
            op.execute(getTestTracingContext(newFs, true));
            abfsConfig.unset(config);
        }
        abfsConfig.set(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS, "false");
    }

    private AbfsRestOperation getMockAbfsRestOp(AbfsRestOperationType opType, AzureBlobFileSystem fs) throws IOException {

        AbfsClient spyClient = Mockito.spy(getAbfsClient(fs.getAbfsStore()));

        // creating the parameters (Url and request headers) to initialize AbfsRestOperation
        AbfsUriQueryBuilder queryBuilder = spyClient.createDefaultUriQueryBuilder();
        URL url = spyClient.createRequestUrl("/", queryBuilder.toString());

        AbfsRestOperation spyRestOp = Mockito.spy(new AbfsRestOperation(opType, spyClient, HTTP_METHOD_HEAD, url, new ArrayList<>()));

        AbfsHttpOperation mockHttpOp = Mockito.spy(spyRestOp.createHttpOperationInstance());
        Mockito.doAnswer(new Answer() {
            private int count = 0;
            @Override
            public Object answer(InvocationOnMock invocationOnMock) {
                if (count++ == 4) {
                    return HTTP_OK;
                } else {
                    return -1;
                }
            }}).when(mockHttpOp).getStatusCode();

        Mockito.doReturn(-1)
                .doReturn(-1)
                .doReturn(-1)
                .doReturn(HTTP_OK)
                .when(mockHttpOp)
                .getStatusCode();
        Mockito.doNothing().when(mockHttpOp).setRequestProperty(nullable(String.class), nullable(String.class));
        Mockito.doNothing().when(mockHttpOp).sendRequest(nullable(byte[].class), nullable(int.class), nullable(int.class));
        Mockito.doNothing().when(mockHttpOp).processResponse(nullable(byte[].class), nullable(int.class), nullable(int.class));

        Mockito.doReturn(mockHttpOp).when(spyRestOp).getResult();

        return spyRestOp;

    }
    public void testInitTimeoutOptimizer(AbfsRestOperationType opType,
                                         int maxRetryCount,
                                         int expectedReqTimeout,
                                         AbfsConfiguration abfsConfig) throws IOException {

        AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(abfsConfig.getRawConfiguration());
        AbfsClient client = fs.getAbfsStore().getClient();
        String query = client.createDefaultUriQueryBuilder().toString();
        URL url = client.createRequestUrl("/testPath", query);
        TimeoutOptimizer opt = new TimeoutOptimizer(url, opType, client.getRetryPolicy(), getConfiguration());
        int retryCount = 0;
        while (retryCount <= maxRetryCount) {
            assertEquals(expectedReqTimeout, opt.getRequestTimeout());
            assertEquals(expectedReqTimeout*1000, opt.getReadTimeout());
            retryCount += 1;
            if (expectedReqTimeout * requestTimeoutIncRate > maxRequestTimeout) {
                expectedReqTimeout = maxRequestTimeout;
            } else {
                expectedReqTimeout *= requestTimeoutIncRate;
            }
            opt.updateRetryTimeout(retryCount);
        }

    }
    private AbfsConfiguration getModifiedTestConfig() throws IOException, IllegalAccessException {
        AbfsConfiguration abfsConfig = getConfiguration();
        abfsConfig.set(ConfigurationKeys.AZURE_OPTIMIZE_TIMEOUTS, "true");
        abfsConfig.set(ConfigurationKeys.AZURE_MAX_REQUEST_TIMEOUT, "90");
        abfsConfig.set(ConfigurationKeys.AZURE_REQUEST_TIMEOUT_INCREASE_RATE, "2");
        maxRequestTimeout = 90;
        requestTimeoutIncRate = 2;
        AbfsConfiguration newConfig = new AbfsConfiguration(abfsConfig.getRawConfiguration(), getAccountName());
        return newConfig;
    }
    private void initOpTypeConfigs() {
        ArrayList<AbfsRestOperationType> opType = new ArrayList<AbfsRestOperationType>(Arrays.asList(
                AbfsRestOperationType.GetFileSystemProperties, AbfsRestOperationType.SetFileSystemProperties,
                AbfsRestOperationType.DeleteFileSystem, AbfsRestOperationType.ListPaths,
                AbfsRestOperationType.CreatePath, AbfsRestOperationType.RenamePath,
                AbfsRestOperationType.GetAcl, AbfsRestOperationType.SetAcl,
                AbfsRestOperationType.GetPathProperties, AbfsRestOperationType.GetPathStatus,
                AbfsRestOperationType.SetOwner, AbfsRestOperationType.SetPermissions,
                AbfsRestOperationType.SetPathProperties, AbfsRestOperationType.CheckAccess
        ));
        ArrayList<Integer> timeoutValues = new ArrayList<Integer>(Arrays.asList(
                new Integer(5), new Integer(5),
                new Integer(5), new Integer(10),
                new Integer(5), new Integer(5),
                new Integer(5), new Integer(5),
                new Integer(5), new Integer(5),
                new Integer(5), new Integer(5),
                new Integer(5), new Integer(5)
        ));
        ArrayList<String> opTypeConfig = new ArrayList<String>(Arrays.asList(
                ConfigurationKeys.AZURE_GET_FS_REQUEST_TIMEOUT, ConfigurationKeys.AZURE_SET_FS_REQUEST_TIMEOUT,
                ConfigurationKeys.AZURE_DELETE_FS_REQUEST_TIMEOUT, ConfigurationKeys.AZURE_LIST_PATH_REQUEST_TIMEOUT,
                ConfigurationKeys.AZURE_CREATE_PATH_REQUEST_TIMEOUT, ConfigurationKeys.AZURE_RENAME_PATH_REQUEST_TIMEOUT,
                ConfigurationKeys.AZURE_GET_ACL_REQUEST_TIMEOUT, ConfigurationKeys.AZURE_SET_ACL_REQUEST_TIMEOUT,
                ConfigurationKeys.AZURE_GET_PATH_PROPERTIES_REQUEST_TIMEOUT, ConfigurationKeys.AZURE_GET_PATH_STATUS_REQUEST_TIMEOUT,
                ConfigurationKeys.AZURE_SET_OWNER_REQUEST_TIMEOUT, ConfigurationKeys.AZURE_SET_PERMISSIONS_REQUEST_TIMEOUT,
                ConfigurationKeys.AZURE_SET_PATH_PROPERTIES_REQUEST_TIMEOUT, ConfigurationKeys.AZURE_CHECK_ACCESS_REQUEST_TIMEOUT
        ));
        for(int i = 0; i < opType.size(); i ++) {
            opMap.put(opType.get(i),timeoutValues.get(i));
            opTimeoutConfigMap.put(opType.get(i), opTypeConfig.get(i));
        }
    }
}
