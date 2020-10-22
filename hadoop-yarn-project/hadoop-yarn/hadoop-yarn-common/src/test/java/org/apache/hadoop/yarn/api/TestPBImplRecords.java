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
package org.apache.hadoop.yarn.api;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.Range;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceProfilesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceTypeInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceTypeInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAttributesToNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAttributesToNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeAttributesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeAttributesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetLabelsToNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetLabelsToNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToAttributesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToAttributesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToLabelsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToLabelsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetResourceProfileRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetResourceProfileResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.IncreaseContainersResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.IncreaseContainersResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.MoveApplicationAcrossQueuesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.MoveApplicationAcrossQueuesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationDeleteRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationDeleteResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationListRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationListResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationSubmissionRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationSubmissionResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationUpdateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationUpdateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ResourceLocalizationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ResourceLocalizationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceAllocationRequest;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationResourceUsageReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerRetryContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ExecutionTypeRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NMTokenPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributeKeyPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributeInfoPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeToAttributeValuePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PreemptionContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PreemptionContractPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PreemptionMessagePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PreemptionResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.QueueInfoPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.QueueUserACLInfoPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceBlacklistRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceOptionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceSizingPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceTypeInfoPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.SchedulingRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.StrictPreemptionContractPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.URLPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.UpdateContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.YarnClusterMetricsPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerRetryContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeKeyProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributeValueProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributesProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionContractProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionMessageProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueUserACLInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceBlacklistRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceOptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceSizingProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SchedulingRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StrictPreemptionContractProto;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnClusterMetricsProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.NodesToAttributesMappingRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshAdminAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResourcesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResourcesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshServiceAclsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceProfilesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncreaseContainersResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncreaseContainersResourceResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.NMTokenProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationDeleteRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationDeleteResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationListRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationListResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationUpdateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationUpdateResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.CheckForDecommissioningNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeToAttributesPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodesToAttributesMappingRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshAdminAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResourcesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResourcesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshNodesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshQueuesRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshQueuesResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshServiceAclsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshServiceAclsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshSuperUserGroupsConfigurationResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RefreshUserToGroupsMappingsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceResponsePBImpl;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * Test class for YARN API protocol records.
 */
public class TestPBImplRecords extends BasePBImplRecordsTest {

  @BeforeClass
  public static void setup() throws Exception {
    typeValueCache.put(Range.class, Range.between(1000L, 2000L));
    typeValueCache.put(URL.class, URL.newInstance(
        "http", "localhost", 8080, "file0"));
    typeValueCache.put(SerializedException.class,
        SerializedException.newInstance(new IOException("exception for test")));
    generateByNewInstance(ExecutionTypeRequest.class);
    typeValueCache.put(ResourceInformation.class, ResourceInformation
        .newInstance("localhost.test/sample", 1l));
    generateByNewInstance(LogAggregationContext.class);
    generateByNewInstance(ApplicationId.class);
    generateByNewInstance(ApplicationAttemptId.class);
    generateByNewInstance(ContainerId.class);
    generateByNewInstance(Resource.class);
    generateByNewInstance(ResourceBlacklistRequest.class);
    generateByNewInstance(ResourceOption.class);
    generateByNewInstance(LocalResource.class);
    generateByNewInstance(Priority.class);
    generateByNewInstance(NodeId.class);
    generateByNewInstance(NodeReport.class);
    generateByNewInstance(Token.class);
    generateByNewInstance(NMToken.class);
    generateByNewInstance(ResourceRequest.class);
    generateByNewInstance(ApplicationAttemptReport.class);
    generateByNewInstance(ApplicationResourceUsageReport.class);
    generateByNewInstance(ApplicationReport.class);
    generateByNewInstance(Container.class);
    generateByNewInstance(ContainerRetryContext.class);
    generateByNewInstance(ContainerLaunchContext.class);
    generateByNewInstance(ApplicationSubmissionContext.class);
    generateByNewInstance(ContainerReport.class);
    generateByNewInstance(UpdateContainerRequest.class);
    generateByNewInstance(UpdateContainerError.class);
    generateByNewInstance(IncreaseContainersResourceRequest.class);
    generateByNewInstance(IncreaseContainersResourceResponse.class);
    generateByNewInstance(ContainerStatus.class);
    generateByNewInstance(PreemptionContainer.class);
    generateByNewInstance(PreemptionResourceRequest.class);
    generateByNewInstance(PreemptionContainer.class);
    generateByNewInstance(PreemptionContract.class);
    generateByNewInstance(StrictPreemptionContract.class);
    generateByNewInstance(PreemptionMessage.class);
    generateByNewInstance(StartContainerRequest.class);
    generateByNewInstance(NodeLabel.class);
    generateByNewInstance(UpdatedContainer.class);
    generateByNewInstance(ContainerUpdateRequest.class);
    generateByNewInstance(ContainerUpdateResponse.class);
    // genByNewInstance does not apply to QueueInfo, cause
    // it is recursive(has sub queues)
    typeValueCache.put(QueueInfo.class, QueueInfo.newInstance("root", 1.0f,
        1.0f, 0.1f, null, null, QueueState.RUNNING, ImmutableSet.of("x", "y"),
        "x && y", null, false, null, false));
    generateByNewInstance(QueueStatistics.class);
    generateByNewInstance(QueueUserACLInfo.class);
    generateByNewInstance(YarnClusterMetrics.class);
    // for reservation system
    generateByNewInstance(ReservationId.class);
    generateByNewInstance(ReservationRequest.class);
    generateByNewInstance(ReservationRequests.class);
    generateByNewInstance(ReservationDefinition.class);
    generateByNewInstance(ResourceAllocationRequest.class);
    generateByNewInstance(ReservationAllocationState.class);
    generateByNewInstance(ResourceUtilization.class);
    generateByNewInstance(ReInitializeContainerRequest.class);
    generateByNewInstance(ReInitializeContainerResponse.class);
    generateByNewInstance(RestartContainerResponse.class);
    generateByNewInstance(RollbackResponse.class);
    generateByNewInstance(CommitResponse.class);
    generateByNewInstance(ApplicationTimeout.class);
    generateByNewInstance(QueueConfigurations.class);
    generateByNewInstance(CollectorInfo.class);
    generateByNewInstance(ResourceTypeInfo.class);
    generateByNewInstance(ResourceSizing.class);
    generateByNewInstance(SchedulingRequest.class);
    generateByNewInstance(RejectedSchedulingRequest.class);
    //for Node attribute support
    generateByNewInstance(NodeAttributeKey.class);
    generateByNewInstance(NodeAttribute.class);
    generateByNewInstance(NodeToAttributes.class);
    generateByNewInstance(NodeToAttributeValue.class);
    generateByNewInstance(NodeAttributeInfo.class);
    generateByNewInstance(NodesToAttributesMappingRequest.class);
  }

  @Test
  public void testAllocateRequestPBImpl() throws Exception {
    validatePBImplRecord(AllocateRequestPBImpl.class, AllocateRequestProto.class);
  }

  @Test
  public void testAllocateResponsePBImpl() throws Exception {
    validatePBImplRecord(AllocateResponsePBImpl.class, AllocateResponseProto.class);
  }

  @Test
  public void testCancelDelegationTokenRequestPBImpl() throws Exception {
    validatePBImplRecord(CancelDelegationTokenRequestPBImpl.class,
        CancelDelegationTokenRequestProto.class);
  }

  @Test
  public void testCancelDelegationTokenResponsePBImpl() throws Exception {
    validatePBImplRecord(CancelDelegationTokenResponsePBImpl.class,
        CancelDelegationTokenResponseProto.class);
  }

  @Test
  public void testFinishApplicationMasterRequestPBImpl() throws Exception {
    validatePBImplRecord(FinishApplicationMasterRequestPBImpl.class,
        FinishApplicationMasterRequestProto.class);
  }

  @Test
  public void testFinishApplicationMasterResponsePBImpl() throws Exception {
    validatePBImplRecord(FinishApplicationMasterResponsePBImpl.class,
        FinishApplicationMasterResponseProto.class);
  }

  @Test
  public void testGetApplicationAttemptReportRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptReportRequestPBImpl.class,
        GetApplicationAttemptReportRequestProto.class);
  }

  @Test
  public void testGetApplicationAttemptReportResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptReportResponsePBImpl.class,
        GetApplicationAttemptReportResponseProto.class);
  }

  @Test
  public void testGetApplicationAttemptsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptsRequestPBImpl.class,
        GetApplicationAttemptsRequestProto.class);
  }

  @Test
  public void testGetApplicationAttemptsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationAttemptsResponsePBImpl.class,
        GetApplicationAttemptsResponseProto.class);
  }

  @Test
  public void testGetApplicationReportRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationReportRequestPBImpl.class,
        GetApplicationReportRequestProto.class);
  }

  @Test
  public void testGetApplicationReportResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationReportResponsePBImpl.class,
        GetApplicationReportResponseProto.class);
  }

  @Test
  public void testGetApplicationsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetApplicationsRequestPBImpl.class,
        GetApplicationsRequestProto.class);
  }

  @Test
  public void testGetApplicationsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetApplicationsResponsePBImpl.class,
        GetApplicationsResponseProto.class);
  }

  @Test
  public void testGetClusterMetricsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetClusterMetricsRequestPBImpl.class,
        GetClusterMetricsRequestProto.class);
  }

  @Test
  public void testGetClusterMetricsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetClusterMetricsResponsePBImpl.class,
        GetClusterMetricsResponseProto.class);
  }

  @Test
  public void testGetClusterNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodesRequestPBImpl.class,
        GetClusterNodesRequestProto.class);
  }

  @Test
  public void testGetClusterNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodesResponsePBImpl.class,
        GetClusterNodesResponseProto.class);
  }

  @Test
  public void testGetContainerReportRequestPBImpl() throws Exception {
    validatePBImplRecord(GetContainerReportRequestPBImpl.class,
        GetContainerReportRequestProto.class);
  }

  @Test
  public void testGetContainerReportResponsePBImpl() throws Exception {
    validatePBImplRecord(GetContainerReportResponsePBImpl.class,
        GetContainerReportResponseProto.class);
  }

  @Test
  public void testGetContainersRequestPBImpl() throws Exception {
    validatePBImplRecord(GetContainersRequestPBImpl.class,
        GetContainersRequestProto.class);
  }

  @Test
  public void testGetContainersResponsePBImpl() throws Exception {
    validatePBImplRecord(GetContainersResponsePBImpl.class,
        GetContainersResponseProto.class);
  }

  @Test
  public void testGetContainerStatusesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetContainerStatusesRequestPBImpl.class,
        GetContainerStatusesRequestProto.class);
  }

  @Test
  public void testGetContainerStatusesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetContainerStatusesResponsePBImpl.class,
        GetContainerStatusesResponseProto.class);
  }

  @Test
  public void testGetDelegationTokenRequestPBImpl() throws Exception {
    validatePBImplRecord(GetDelegationTokenRequestPBImpl.class,
        GetDelegationTokenRequestProto.class);
  }

  @Test
  public void testGetDelegationTokenResponsePBImpl() throws Exception {
    validatePBImplRecord(GetDelegationTokenResponsePBImpl.class,
        GetDelegationTokenResponseProto.class);
  }

  @Test
  public void testGetNewApplicationRequestPBImpl() throws Exception {
    validatePBImplRecord(GetNewApplicationRequestPBImpl.class,
        GetNewApplicationRequestProto.class);
  }

  @Test
  public void testGetNewApplicationResponsePBImpl() throws Exception {
    validatePBImplRecord(GetNewApplicationResponsePBImpl.class,
        GetNewApplicationResponseProto.class);
  }

  @Test
  public void testGetQueueInfoRequestPBImpl() throws Exception {
    validatePBImplRecord(GetQueueInfoRequestPBImpl.class,
        GetQueueInfoRequestProto.class);
  }

  @Test
  public void testGetQueueInfoResponsePBImpl() throws Exception {
    validatePBImplRecord(GetQueueInfoResponsePBImpl.class,
        GetQueueInfoResponseProto.class);
  }

  @Test
  public void testGetQueueUserAclsInfoRequestPBImpl() throws Exception {
    validatePBImplRecord(GetQueueUserAclsInfoRequestPBImpl.class,
        GetQueueUserAclsInfoRequestProto.class);
  }

  @Test
  public void testGetQueueUserAclsInfoResponsePBImpl() throws Exception {
    validatePBImplRecord(GetQueueUserAclsInfoResponsePBImpl.class,
        GetQueueUserAclsInfoResponseProto.class);
  }

  @Test
  public void testKillApplicationRequestPBImpl() throws Exception {
    validatePBImplRecord(KillApplicationRequestPBImpl.class,
        KillApplicationRequestProto.class);
  }

  @Test
  public void testKillApplicationResponsePBImpl() throws Exception {
    validatePBImplRecord(KillApplicationResponsePBImpl.class,
        KillApplicationResponseProto.class);
  }

  @Test
  public void testMoveApplicationAcrossQueuesRequestPBImpl() throws Exception {
    validatePBImplRecord(MoveApplicationAcrossQueuesRequestPBImpl.class,
        MoveApplicationAcrossQueuesRequestProto.class);
  }

  @Test
  public void testMoveApplicationAcrossQueuesResponsePBImpl() throws Exception {
    validatePBImplRecord(MoveApplicationAcrossQueuesResponsePBImpl.class,
        MoveApplicationAcrossQueuesResponseProto.class);
  }

  @Test
  public void testRegisterApplicationMasterRequestPBImpl() throws Exception {
    validatePBImplRecord(RegisterApplicationMasterRequestPBImpl.class,
        RegisterApplicationMasterRequestProto.class);
  }

  @Test
  public void testRegisterApplicationMasterResponsePBImpl() throws Exception {
    validatePBImplRecord(RegisterApplicationMasterResponsePBImpl.class,
        RegisterApplicationMasterResponseProto.class);
  }

  @Test
  public void testRenewDelegationTokenRequestPBImpl() throws Exception {
    validatePBImplRecord(RenewDelegationTokenRequestPBImpl.class,
        RenewDelegationTokenRequestProto.class);
  }

  @Test
  public void testRenewDelegationTokenResponsePBImpl() throws Exception {
    validatePBImplRecord(RenewDelegationTokenResponsePBImpl.class,
        RenewDelegationTokenResponseProto.class);
  }

  @Test
  public void testStartContainerRequestPBImpl() throws Exception {
    validatePBImplRecord(StartContainerRequestPBImpl.class,
        StartContainerRequestProto.class);
  }

  @Test
  public void testStartContainersRequestPBImpl() throws Exception {
    validatePBImplRecord(StartContainersRequestPBImpl.class,
        StartContainersRequestProto.class);
  }

  @Test
  public void testStartContainersResponsePBImpl() throws Exception {
    validatePBImplRecord(StartContainersResponsePBImpl.class,
        StartContainersResponseProto.class);
  }

  @Test
  public void testStopContainersRequestPBImpl() throws Exception {
    validatePBImplRecord(StopContainersRequestPBImpl.class,
        StopContainersRequestProto.class);
  }

  @Test
  public void testStopContainersResponsePBImpl() throws Exception {
    validatePBImplRecord(StopContainersResponsePBImpl.class,
        StopContainersResponseProto.class);
  }

  @Test
  public void testIncreaseContainersResourceRequestPBImpl() throws Exception {
    validatePBImplRecord(IncreaseContainersResourceRequestPBImpl.class,
        IncreaseContainersResourceRequestProto.class);
  }

  @Test
  public void testIncreaseContainersResourceResponsePBImpl() throws Exception {
    validatePBImplRecord(IncreaseContainersResourceResponsePBImpl.class,
        IncreaseContainersResourceResponseProto.class);
  }

  @Test
  public void testSubmitApplicationRequestPBImpl() throws Exception {
    validatePBImplRecord(SubmitApplicationRequestPBImpl.class,
        SubmitApplicationRequestProto.class);
  }

  @Test
  public void testSubmitApplicationResponsePBImpl() throws Exception {
    validatePBImplRecord(SubmitApplicationResponsePBImpl.class,
        SubmitApplicationResponseProto.class);
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testApplicationAttemptIdPBImpl() throws Exception {
    validatePBImplRecord(ApplicationAttemptIdPBImpl.class,
        ApplicationAttemptIdProto.class);
  }

  @Test
  public void testApplicationAttemptReportPBImpl() throws Exception {
    validatePBImplRecord(ApplicationAttemptReportPBImpl.class,
        ApplicationAttemptReportProto.class);
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testApplicationIdPBImpl() throws Exception {
    validatePBImplRecord(ApplicationIdPBImpl.class, ApplicationIdProto.class);
  }

  @Test
  public void testApplicationReportPBImpl() throws Exception {
    validatePBImplRecord(ApplicationReportPBImpl.class,
        ApplicationReportProto.class);
  }

  @Test
  public void testApplicationResourceUsageReportPBImpl() throws Exception {
    excludedPropertiesMap.put(ApplicationResourceUsageReportPBImpl.class.getClass(),
        Arrays.asList("PreemptedResourceSecondsMap", "ResourceSecondsMap"));
    validatePBImplRecord(ApplicationResourceUsageReportPBImpl.class,
        ApplicationResourceUsageReportProto.class);
  }

  @Test
  public void testApplicationSubmissionContextPBImpl() throws Exception {
    validatePBImplRecord(ApplicationSubmissionContextPBImpl.class,
        ApplicationSubmissionContextProto.class);
    
    ApplicationSubmissionContext ctx =
        ApplicationSubmissionContext.newInstance(null, null, null, null, null,
            false, false, 0, Resources.none(), null, false, null, null);
    
    Assert.assertNotNull(ctx.getResource());
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testContainerIdPBImpl() throws Exception {
    validatePBImplRecord(ContainerIdPBImpl.class, ContainerIdProto.class);
  }

  @Test
  public void testContainerRetryPBImpl() throws Exception {
    validatePBImplRecord(ContainerRetryContextPBImpl.class,
        ContainerRetryContextProto.class);
  }

  @Test
  public void testContainerLaunchContextPBImpl() throws Exception {
    validatePBImplRecord(ContainerLaunchContextPBImpl.class,
        ContainerLaunchContextProto.class);
  }

  @Test
  public void testResourceLocalizationRequest() throws Exception {
    validatePBImplRecord(ResourceLocalizationRequestPBImpl.class,
        YarnServiceProtos.ResourceLocalizationRequestProto.class);
  }

  @Test
  public void testResourceLocalizationResponse() throws Exception {
    validatePBImplRecord(ResourceLocalizationResponsePBImpl.class,
        YarnServiceProtos.ResourceLocalizationResponseProto.class);
  }

  @Test
  public void testContainerPBImpl() throws Exception {
    validatePBImplRecord(ContainerPBImpl.class, ContainerProto.class);
  }

  @Test
  public void testContainerReportPBImpl() throws Exception {
    validatePBImplRecord(ContainerReportPBImpl.class, ContainerReportProto.class);
  }

  @Test
  public void testUpdateContainerRequestPBImpl() throws Exception {
    validatePBImplRecord(UpdateContainerRequestPBImpl.class,
        YarnServiceProtos.UpdateContainerRequestProto.class);
  }

  @Test
  public void testContainerStatusPBImpl() throws Exception {
    validatePBImplRecord(ContainerStatusPBImpl.class, ContainerStatusProto.class);
  }

  @Test
  public void testLocalResourcePBImpl() throws Exception {
    validatePBImplRecord(LocalResourcePBImpl.class, LocalResourceProto.class);
  }

  @Test
  public void testNMTokenPBImpl() throws Exception {
    validatePBImplRecord(NMTokenPBImpl.class, NMTokenProto.class);
  }

  @Test
  @Ignore
  // ignore cause ApplicationIdPBImpl is immutable
  public void testNodeIdPBImpl() throws Exception {
    validatePBImplRecord(NodeIdPBImpl.class, NodeIdProto.class);
  }

  @Test
  public void testNodeReportPBImpl() throws Exception {
    validatePBImplRecord(NodeReportPBImpl.class, NodeReportProto.class);
  }

  @Test
  public void testPreemptionContainerPBImpl() throws Exception {
    validatePBImplRecord(PreemptionContainerPBImpl.class,
        PreemptionContainerProto.class);
  }

  @Test
  public void testPreemptionContractPBImpl() throws Exception {
    validatePBImplRecord(PreemptionContractPBImpl.class,
        PreemptionContractProto.class);
  }

  @Test
  public void testPreemptionMessagePBImpl() throws Exception {
    validatePBImplRecord(PreemptionMessagePBImpl.class,
        PreemptionMessageProto.class);
  }

  @Test
  public void testPreemptionResourceRequestPBImpl() throws Exception {
    validatePBImplRecord(PreemptionResourceRequestPBImpl.class,
        PreemptionResourceRequestProto.class);
  }

  @Test
  public void testPriorityPBImpl() throws Exception {
    validatePBImplRecord(PriorityPBImpl.class, PriorityProto.class);
  }

  @Test
  public void testQueueInfoPBImpl() throws Exception {
    validatePBImplRecord(QueueInfoPBImpl.class, QueueInfoProto.class);
  }

  @Test
  public void testQueueUserACLInfoPBImpl() throws Exception {
    validatePBImplRecord(QueueUserACLInfoPBImpl.class,
        QueueUserACLInfoProto.class);
  }

  @Test
  public void testResourceBlacklistRequestPBImpl() throws Exception {
    validatePBImplRecord(ResourceBlacklistRequestPBImpl.class,
        ResourceBlacklistRequestProto.class);
  }

  @Test
  @Ignore
  // ignore as ResourceOptionPBImpl is immutable
  public void testResourceOptionPBImpl() throws Exception {
    validatePBImplRecord(ResourceOptionPBImpl.class, ResourceOptionProto.class);
  }

  @Test
  public void testResourcePBImpl() throws Exception {
    validatePBImplRecord(ResourcePBImpl.class, ResourceProto.class);
  }

  @Test
  public void testResourceRequestPBImpl() throws Exception {
    validatePBImplRecord(ResourceRequestPBImpl.class, ResourceRequestProto.class);
  }

  @Test
  public void testResourceSizingPBImpl() throws Exception {
    validatePBImplRecord(ResourceSizingPBImpl.class, ResourceSizingProto.class);
  }

  @Test
  public void testSchedulingRequestPBImpl() throws Exception {
    validatePBImplRecord(SchedulingRequestPBImpl.class,
        SchedulingRequestProto.class);
  }

  @Test
  public void testSerializedExceptionPBImpl() throws Exception {
    validatePBImplRecord(SerializedExceptionPBImpl.class,
        SerializedExceptionProto.class);
  }

  @Test
  public void testStrictPreemptionContractPBImpl() throws Exception {
    validatePBImplRecord(StrictPreemptionContractPBImpl.class,
        StrictPreemptionContractProto.class);
  }

  @Test
  public void testTokenPBImpl() throws Exception {
    validatePBImplRecord(TokenPBImpl.class, TokenProto.class);
  }

  @Test
  public void testURLPBImpl() throws Exception {
    validatePBImplRecord(URLPBImpl.class, URLProto.class);
  }

  @Test
  public void testYarnClusterMetricsPBImpl() throws Exception {
    validatePBImplRecord(YarnClusterMetricsPBImpl.class,
        YarnClusterMetricsProto.class);
  }

  @Test
  public void testRefreshAdminAclsRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshAdminAclsRequestPBImpl.class,
        RefreshAdminAclsRequestProto.class);
  }

  @Test
  public void testRefreshAdminAclsResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshAdminAclsResponsePBImpl.class,
        RefreshAdminAclsResponseProto.class);
  }

  @Test
  public void testRefreshNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshNodesRequestPBImpl.class,
        RefreshNodesRequestProto.class);
  }

  @Test
  public void testRefreshNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshNodesResponsePBImpl.class,
        RefreshNodesResponseProto.class);
  }

  @Test
  public void testRefreshQueuesRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshQueuesRequestPBImpl.class,
        RefreshQueuesRequestProto.class);
  }

  @Test
  public void testRefreshQueuesResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshQueuesResponsePBImpl.class,
        RefreshQueuesResponseProto.class);
  }

  @Test
  public void testRefreshNodesResourcesRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshNodesResourcesRequestPBImpl.class,
        RefreshNodesResourcesRequestProto.class);
  }

  @Test
  public void testRefreshNodesResourcesResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshNodesResourcesResponsePBImpl.class,
        RefreshNodesResourcesResponseProto.class);
  }

  @Test
  public void testRefreshServiceAclsRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshServiceAclsRequestPBImpl.class,
        RefreshServiceAclsRequestProto.class);
  }

  @Test
  public void testRefreshServiceAclsResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshServiceAclsResponsePBImpl.class,
        RefreshServiceAclsResponseProto.class);
  }

  @Test
  public void testRefreshSuperUserGroupsConfigurationRequestPBImpl()
      throws Exception {
    validatePBImplRecord(RefreshSuperUserGroupsConfigurationRequestPBImpl.class,
        RefreshSuperUserGroupsConfigurationRequestProto.class);
  }

  @Test
  public void testRefreshSuperUserGroupsConfigurationResponsePBImpl()
      throws Exception {
    validatePBImplRecord(RefreshSuperUserGroupsConfigurationResponsePBImpl.class,
        RefreshSuperUserGroupsConfigurationResponseProto.class);
  }

  @Test
  public void testRefreshUserToGroupsMappingsRequestPBImpl() throws Exception {
    validatePBImplRecord(RefreshUserToGroupsMappingsRequestPBImpl.class,
        RefreshUserToGroupsMappingsRequestProto.class);
  }

  @Test
  public void testRefreshUserToGroupsMappingsResponsePBImpl() throws Exception {
    validatePBImplRecord(RefreshUserToGroupsMappingsResponsePBImpl.class,
        RefreshUserToGroupsMappingsResponseProto.class);
  }

  @Test
  public void testUpdateNodeResourceRequestPBImpl() throws Exception {
    validatePBImplRecord(UpdateNodeResourceRequestPBImpl.class,
        UpdateNodeResourceRequestProto.class);
  }

  @Test
  public void testUpdateNodeResourceResponsePBImpl() throws Exception {
    validatePBImplRecord(UpdateNodeResourceResponsePBImpl.class,
        UpdateNodeResourceResponseProto.class);
  }

  @Test
  public void testReservationSubmissionRequestPBImpl() throws Exception {
    validatePBImplRecord(ReservationSubmissionRequestPBImpl.class,
        ReservationSubmissionRequestProto.class);
  }

  @Test
  public void testReservationSubmissionResponsePBImpl() throws Exception {
    validatePBImplRecord(ReservationSubmissionResponsePBImpl.class,
        ReservationSubmissionResponseProto.class);
  }

  @Test
  public void testReservationUpdateRequestPBImpl() throws Exception {
    validatePBImplRecord(ReservationUpdateRequestPBImpl.class,
        ReservationUpdateRequestProto.class);
  }

  @Test
  public void testReservationUpdateResponsePBImpl() throws Exception {
    validatePBImplRecord(ReservationUpdateResponsePBImpl.class,
        ReservationUpdateResponseProto.class);
  }

  @Test
  public void testReservationDeleteRequestPBImpl() throws Exception {
    validatePBImplRecord(ReservationDeleteRequestPBImpl.class,
        ReservationDeleteRequestProto.class);
  }

  @Test
  public void testReservationDeleteResponsePBImpl() throws Exception {
    validatePBImplRecord(ReservationDeleteResponsePBImpl.class,
        ReservationDeleteResponseProto.class);
  }

  @Test
  public void testReservationListRequestPBImpl() throws Exception {
    validatePBImplRecord(ReservationListRequestPBImpl.class,
            ReservationListRequestProto.class);
  }

  @Test
  public void testReservationListResponsePBImpl() throws Exception {
    validatePBImplRecord(ReservationListResponsePBImpl.class,
            ReservationListResponseProto.class);
  }

  @Test
  public void testAddToClusterNodeLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(AddToClusterNodeLabelsRequestPBImpl.class,
        AddToClusterNodeLabelsRequestProto.class);
  }
  
  @Test
  public void testAddToClusterNodeLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(AddToClusterNodeLabelsResponsePBImpl.class,
        AddToClusterNodeLabelsResponseProto.class);
  }
  
  @Test
  public void testRemoveFromClusterNodeLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(RemoveFromClusterNodeLabelsRequestPBImpl.class,
        RemoveFromClusterNodeLabelsRequestProto.class);
  }
  
  @Test
  public void testRemoveFromClusterNodeLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(RemoveFromClusterNodeLabelsResponsePBImpl.class,
        RemoveFromClusterNodeLabelsResponseProto.class);
  }
  
  @Test
  public void testGetClusterNodeLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodeLabelsRequestPBImpl.class,
        GetClusterNodeLabelsRequestProto.class);
  }

  @Test
  public void testGetClusterNodeLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodeLabelsResponsePBImpl.class,
        GetClusterNodeLabelsResponseProto.class);
  }
  
  @Test
  public void testReplaceLabelsOnNodeRequestPBImpl() throws Exception {
    validatePBImplRecord(ReplaceLabelsOnNodeRequestPBImpl.class,
        ReplaceLabelsOnNodeRequestProto.class);
  }

  @Test
  public void testReplaceLabelsOnNodeResponsePBImpl() throws Exception {
    validatePBImplRecord(ReplaceLabelsOnNodeResponsePBImpl.class,
        ReplaceLabelsOnNodeResponseProto.class);
  }
  
  @Test
  public void testGetNodeToLabelsRequestPBImpl() throws Exception {
    validatePBImplRecord(GetNodesToLabelsRequestPBImpl.class,
        GetNodesToLabelsRequestProto.class);
  }

  @Test
  public void testGetNodeToLabelsResponsePBImpl() throws Exception {
    validatePBImplRecord(GetNodesToLabelsResponsePBImpl.class,
        GetNodesToLabelsResponseProto.class);
  }

  @Test
  public void testGetLabelsToNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetLabelsToNodesRequestPBImpl.class,
        GetLabelsToNodesRequestProto.class);
  }

  @Test
  public void testGetLabelsToNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetLabelsToNodesResponsePBImpl.class,
        GetLabelsToNodesResponseProto.class);
  }
  
  @Test
  public void testNodeLabelAttributesPBImpl() throws Exception {
    validatePBImplRecord(NodeLabelPBImpl.class,
        NodeLabelProto.class);
  }
  
  @Test
  public void testCheckForDecommissioningNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(CheckForDecommissioningNodesRequestPBImpl.class,
        CheckForDecommissioningNodesRequestProto.class);
  }

  @Test
  public void testCheckForDecommissioningNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(CheckForDecommissioningNodesResponsePBImpl.class,
        CheckForDecommissioningNodesResponseProto.class);
  }

  @Test
  public void testExecutionTypeRequestPBImpl() throws Exception {
    validatePBImplRecord(ExecutionTypeRequestPBImpl.class,
        ExecutionTypeRequestProto.class);
  }

  @Test
  public void testGetAllResourceProfilesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetAllResourceProfilesResponsePBImpl.class,
        GetAllResourceProfilesResponseProto.class);
  }

  @Test
  public void testGetResourceProfileRequestPBImpl() throws Exception {
    validatePBImplRecord(GetResourceProfileRequestPBImpl.class,
        GetResourceProfileRequestProto.class);
  }

  @Test
  public void testGetResourceProfileResponsePBImpl() throws Exception {
    validatePBImplRecord(GetResourceProfileResponsePBImpl.class,
        GetResourceProfileResponseProto.class);
  }

  @Test
  public void testResourceTypesInfoPBImpl() throws Exception {
    validatePBImplRecord(ResourceTypeInfoPBImpl.class,
        YarnProtos.ResourceTypeInfoProto.class);
  }

  @Test
  public void testGetAllResourceTypesInfoRequestPBImpl() throws Exception {
    validatePBImplRecord(GetAllResourceTypeInfoRequestPBImpl.class,
        YarnServiceProtos.GetAllResourceTypeInfoRequestProto.class);
  }

  @Test
  public void testGetAllResourceTypesInfoResponsePBImpl() throws Exception {
    validatePBImplRecord(GetAllResourceTypeInfoResponsePBImpl.class,
        YarnServiceProtos.GetAllResourceTypeInfoResponseProto.class);
  }

  @Test
  public void testNodeAttributeKeyPBImpl() throws Exception {
    validatePBImplRecord(NodeAttributeKeyPBImpl.class,
        NodeAttributeKeyProto.class);
  }

  @Test
  public void testNodeToAttributeValuePBImpl() throws Exception {
    validatePBImplRecord(NodeToAttributeValuePBImpl.class,
        NodeToAttributeValueProto.class);
  }

  @Test
  public void testNodeAttributePBImpl() throws Exception {
    validatePBImplRecord(NodeAttributePBImpl.class, NodeAttributeProto.class);
  }

  @Test
  public void testNodeAttributeInfoPBImpl() throws Exception {
    validatePBImplRecord(NodeAttributeInfoPBImpl.class,
        NodeAttributeInfoProto.class);
  }

  @Test
  public void testNodeToAttributesPBImpl() throws Exception {
    validatePBImplRecord(NodeToAttributesPBImpl.class,
        NodeToAttributesProto.class);
  }

  @Test
  public void testNodesToAttributesMappingRequestPBImpl() throws Exception {
    validatePBImplRecord(NodesToAttributesMappingRequestPBImpl.class,
        NodesToAttributesMappingRequestProto.class);
  }

  @Test
  public void testGetAttributesToNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetAttributesToNodesRequestPBImpl.class,
        YarnServiceProtos.GetAttributesToNodesRequestProto.class);
  }

  @Test
  public void testGetAttributesToNodesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetAttributesToNodesResponsePBImpl.class,
        YarnServiceProtos.GetAttributesToNodesResponseProto.class);
  }

  @Test
  public void testGetClusterNodeAttributesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodeAttributesRequestPBImpl.class,
        YarnServiceProtos.GetClusterNodeAttributesRequestProto.class);
  }

  @Test
  public void testGetClusterNodeAttributesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetClusterNodeAttributesResponsePBImpl.class,
        YarnServiceProtos.GetClusterNodeAttributesResponseProto.class);
  }

  @Test
  public void testGetNodesToAttributesRequestPBImpl() throws Exception {
    validatePBImplRecord(GetNodesToAttributesRequestPBImpl.class,
        YarnServiceProtos.GetNodesToAttributesRequestProto.class);
  }

  @Test
  public void testGetNodesToAttributesResponsePBImpl() throws Exception {
    validatePBImplRecord(GetNodesToAttributesResponsePBImpl.class,
        YarnServiceProtos.GetNodesToAttributesResponseProto.class);
  }
}
