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

package org.apache.hadoop.fs.s3a.impl;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

public class AddRequestHeaderInterceptor implements ExecutionInterceptor {
    private final Map<String, Consumer<AwsRequestOverrideConfiguration.Builder>> appliers = new HashMap<>();

    public AddRequestHeaderInterceptor(Map<String, Map<String, List<String>>> requestHeaders) {
        requestHeaders.forEach((request, headers) -> appliers
                .put(request.toLowerCase(Locale.ROOT), (b) -> headers.forEach(b::putHeader))
        );
    }

    public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes executionAttributes) {
        assert context.request() instanceof AwsRequest;

        AwsRequest request = (AwsRequest) context.request();
        String requestName = request.getClass().getSimpleName().toLowerCase(Locale.ROOT);
        Consumer<AwsRequestOverrideConfiguration.Builder> applier = appliers.get(requestName);

        if (applier != null) {
            AwsRequestOverrideConfiguration overrideConfiguration =
                    request.overrideConfiguration()
                            .map(AwsRequestOverrideConfiguration::toBuilder)
                            .orElseGet(AwsRequestOverrideConfiguration::builder)
                            .applyMutation(applier)
                            .build();
            return request.toBuilder().overrideConfiguration(overrideConfiguration).build();
        } else {
            return request;
        }
    }
}
