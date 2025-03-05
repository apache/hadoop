/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hdfs.web;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.UnexpectedServerException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;

import java.io.IOException;
import java.net.URI;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;


public class TestWebHDFSResponse {

    private ClientAndServer mockWebHDFS;

    private final static String WEBHDFS_HOST = "localhost";
    private final static int WEBHDFS_PORT = 8552;
    private final static URI WEBHDFS_URI = URI.create("webhdfs://" + WEBHDFS_HOST + ":" + WEBHDFS_PORT);
    private final static Header CONTENT_TYPE_APPLICATION_JSON = new Header("Content-Type", "application/json");
    private final static String TEST_WEBHDFS_PATH = "/test1/test2";
    final HttpRequest FILE_SYSTEM_REQUEST = request()
            .withMethod("GET")
            .withPath(WebHdfsFileSystem.PATH_PREFIX + TEST_WEBHDFS_PATH);

    @Before
    public void startMockWebHDFSServer() {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir"));
        mockWebHDFS = startClientAndServer(WEBHDFS_PORT);
    }

    @Test
    public void testSuccess() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_OK)
                            .withHeaders(CONTENT_TYPE_APPLICATION_JSON)
                            .withBody("{\n" +
                                    "  \"FileStatuses\": {\n" +
                                    "    \"FileStatus\": [{\n" +
                                    "      \"accessTime\": 1320171722771,\n" +
                                    "      \"blockSize\": 33554432,\n" +
                                    "      \"group\": \"supergroup\",\n" +
                                    "      \"length\": 24930,\n" +
                                    "      \"modificationTime\": 1320171722771,\n" +
                                    "      \"owner\": \"webuser\",\n" +
                                    "      \"pathSuffix\": \"a.patch\",\n" +
                                    "      \"permission\": \"644\",\n" +
                                    "      \"replication\": 1,\n" +
                                    "      \"type\": \"FILE\"\n" +
                                    "    }]\n" +
                                    "  }\n" +
                                    "}\n"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());
                fs.listStatus(new Path(TEST_WEBHDFS_PATH));
            }
        }
    }

    @Test(expected = AccessControlException.class)
    public void testUnAuthorized() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_UNAUTHORIZED));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());
                fs.listStatus(new Path(TEST_WEBHDFS_PATH));
            }
        }
    }

    @Test
    public void testUnexpectedResponse() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                            .withBody("Unexpected error occurred"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());

                final Exception e = assertThrows(IOException.class,
                        () -> fs.listStatus(new Path(TEST_WEBHDFS_PATH)));

                assertEquals(JsonParseException.class, e.getCause().getClass());
            }
        }
    }

    @Test
    public void testEmptyResponse() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());

                final Exception e = assertThrows(IOException.class,
                        () -> fs.listStatus(new Path(TEST_WEBHDFS_PATH)));

                assertNull(e.getCause());
            }
        }
    }

    @Test
    public void testNonRemoteExceptions() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            final int expectedStatus = HttpStatus.SC_BAD_REQUEST;

            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(expectedStatus)
                            .withHeaders(CONTENT_TYPE_APPLICATION_JSON)
                            .withBody("{\n" +
                                    "  \"UnexpectedServerException\": {\n" +
                                    "    \"javaClassName\": \"" + UnexpectedServerException.class.getName() + "\",\n" +
                                    "    \"exception\": \"" + UnexpectedServerException.class.getSimpleName() + "\",\n" +
                                    "    \"message\": \"Unexpected exception thrown\"\n" +
                                    "  }\n" +
                                    "}"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());

                final Exception e = assertThrows(IOException.class,
                        () -> fs.listStatus(new Path(TEST_WEBHDFS_PATH)));

                assertThat(e.getMessage(), startsWith(WEBHDFS_HOST + ":" + WEBHDFS_PORT + ": Server returned HTTP response code: " + expectedStatus));
            }
        }
    }

    @Test
    public void testExceptionMessageIsNull() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                            .withHeaders(CONTENT_TYPE_APPLICATION_JSON)
                            .withBody("{\n" +
                                    "  \"RemoteException\": {\n" +
                                    "    \"javaClassName\": \"" + Exception.class.getName() + "\",\n" +
                                    "    \"exception\": \"" + Exception.class.getSimpleName() + "\"\n" +
                                    "  }\n" +
                                    "}"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());

                final Exception e = assertThrows(IOException.class,
                        () -> fs.listStatus(new Path(TEST_WEBHDFS_PATH)));

                assertNull(e.getMessage());
            }
        }
    }

    @Test
    public void testStandbyException() throws IOException {
        final String reMessage = "Server returned: " + StandbyException.class.getSimpleName();

        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_FORBIDDEN)
                            .withHeaders(CONTENT_TYPE_APPLICATION_JSON)
                            .withBody("{\n" +
                                    "  \"RemoteException\": {\n" +
                                    "    \"javaClassName\": \"" + StandbyException.class.getName() + "\",\n" +
                                    "    \"exception\": \"" + StandbyException.class.getSimpleName() + "\",\n" +
                                    "    \"message\": \"" + reMessage + "\"\n" +
                                    "  }\n" +
                                    "}"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());

                final Exception e = assertThrows(IOException.class,
                        () -> fs.listStatus(new Path(TEST_WEBHDFS_PATH)));

                assertEquals(WEBHDFS_HOST + ":" + WEBHDFS_PORT + ": " + RemoteException.class.getName() +
                                "(" + StandbyException.class.getName() + "): " + reMessage,
                        e.getMessage());
            }
        }
    }

    @Test(expected = SecretManager.InvalidToken.class)
    public void testInvalidToken() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            final String reMessage = SecurityUtil.FAILED_TO_GET_UGI_MSG_HEADER + " " +
                    SecretManager.InvalidToken.class.getName() + ": " +
                    "token (token for test_user: HDFS_DELEGATION_TOKEN owner=test_user, renewer=test_user, " +
                    "realUser=test_user, issueDate=0, maxDate=99999, sequenceNumber=9999, masterKeyId=999) can't be found in cache";

            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_FORBIDDEN)
                            .withHeaders(CONTENT_TYPE_APPLICATION_JSON)
                            .withBody("{\n" +
                                    "  \"RemoteException\": {\n" +
                                    "    \"javaClassName\": \"" + SecretManager.InvalidToken.class.getName() + "\",\n" +
                                    "    \"exception\": \"" + SecretManager.InvalidToken.class.getSimpleName() + "\",\n" +
                                    "    \"message\": \"" + reMessage + "\"\n" +
                                    "  }\n" +
                                    "}"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());
                fs.listStatus(new Path(TEST_WEBHDFS_PATH));
            }
        }
    }

    @Test(expected = SecretManager.InvalidToken.class)
    public void testInvalidTokenForHttpFS() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            final String reMessage = SecretManager.InvalidToken.class.getName() + ": " +
                    "token (token for test_user: HDFS_DELEGATION_TOKEN owner=test_user, renewer=test_user, " +
                    "realUser=test_user, issueDate=0, maxDate=99999, sequenceNumber=9999, masterKeyId=999) can't be found in cache";

            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_FORBIDDEN)
                            .withHeaders(CONTENT_TYPE_APPLICATION_JSON)
                            .withBody("{\n" +
                                    "  \"RemoteException\": {\n" +
                                    "    \"javaClassName\": \"" + AuthenticationException.class.getName() + "\",\n" +
                                    "    \"exception\": \"" + AuthenticationException.class.getSimpleName() + "\",\n" +
                                    "    \"message\": \"" + reMessage + "\"\n" +
                                    "  }\n" +
                                    "}"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());
                fs.listStatus(new Path(TEST_WEBHDFS_PATH));
            }
        }
    }

    @Test(expected = RemoteException.class)
    public void testOtherRemoteException() throws IOException {
        try (final MockServerClient mockWebHDFSServerClient = new MockServerClient(WEBHDFS_HOST, WEBHDFS_PORT)) {
            mockWebHDFSServerClient
                    .when(FILE_SYSTEM_REQUEST, exactly(1))
                    .respond(response()
                            .withStatusCode(HttpStatus.SC_FORBIDDEN)
                            .withHeaders(CONTENT_TYPE_APPLICATION_JSON)
                            .withBody("{\n" +
                                    "  \"RemoteException\": {\n" +
                                    "    \"javaClassName\": \"" + Exception.class.getName() + "\",\n" +
                                    "    \"exception\": \"" + Exception.class.getSimpleName() + "\",\n" +
                                    "    \"message\": \"Other RemoteException\"\n" +
                                    "  }\n" +
                                    "}"));

            try (FileSystem fs = new WebHdfsFileSystem()) {
                fs.initialize(WEBHDFS_URI, new Configuration());
                fs.listStatus(new Path(TEST_WEBHDFS_PATH));
            }
        }
    }

    @After
    public void stopMockWebHDFSServer() {
        mockWebHDFS.stop();
    }

}
