/*
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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;

/**
 * This credential provider has jittered between existing and non-existing,
 * but it turns up in documentation enough that it has been restored.
 * It extends {@link IAMInstanceCredentialsProvider} to pick up its
 * bindings, which are currently to use the
 * {@code EC2ContainerCredentialsProviderWrapper} class for IAM and container
 * authentication.
 * <p>
 * When it fails to authenticate, it raises a
 * {@link NoAwsCredentialsException} which can be recognized by retry handlers
 * as a non-recoverable failure.
 * <p>
 * It is implicitly public; marked evolving as we can change its semantics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class SharedInstanceCredentialProvider extends
    IAMInstanceCredentialsProvider {
}
