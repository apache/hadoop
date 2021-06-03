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

package org.apache.hadoop.io;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.junit.Assert;
import org.junit.Test;

public class TestIOUtilsWrapExceptionSuite extends Assert {
    @Test
    public void testWrapExceptionWithInterruptedException() throws Exception {
        InterruptedIOException inputException = new InterruptedIOException("message");
        NullPointerException causeException = new NullPointerException("cause");
        inputException.initCause(causeException);
        Exception outputException = IOUtils.wrapException("path", "methodName", inputException);

        // The new exception should retain the input message, cause, and type
        assertTrue(outputException instanceof InterruptedIOException);
        assertTrue(outputException.getCause() instanceof NullPointerException);
        assertEquals(outputException.getMessage(), inputException.getMessage());
        assertEquals(outputException.getCause(), inputException.getCause());
    }

    @Test
    public void testWrapExceptionWithInterruptedCauseException() throws Exception {
        IOException inputException = new IOException("message");
        InterruptedException causeException = new InterruptedException("cause");
        inputException.initCause(causeException);
        Exception outputException = IOUtils.wrapException("path", "methodName", inputException);

        // The new exception should retain the input message and cause
        // but be an InterruptedIOException because the cause was an InterruptedException
        assertTrue(outputException instanceof InterruptedIOException);
        assertTrue(outputException.getCause() instanceof InterruptedException);
        assertEquals(outputException.getMessage(), inputException.getMessage());
        assertEquals(outputException.getCause(), inputException.getCause());
    }
}
