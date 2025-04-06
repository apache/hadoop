package org.apache.hadoop.yarn.server.scheduler;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;


public class TestOpportunisticContainerContext {

    @Spy
    OpportunisticContainerContext opportunisticContainerContext;

    Map<Resource, OpportunisticContainerAllocator.EnrichedResourceRequest>
            reqMap = new HashMap<>();

    TreeMap<SchedulerRequestKey, Map<Resource, OpportunisticContainerAllocator.EnrichedResourceRequest>>
            outstandingOpReqs;

    @Before
    public void SetUp() {
        opportunisticContainerContext = Mockito.spy(new OpportunisticContainerContext());
        outstandingOpReqs = new TreeMap<>();
    }

    /*
     Resource Request - {
        Location = ANY
        No of container != 0
     }
     */
    @Test
    public void testAddToOutstandingReqsWithANYRequest() {
        ResourceRequest request = getResourceRequest(ResourceRequest.ANY, 1);
        List<ResourceRequest> resourceRequestList = new ArrayList<>();
        resourceRequestList.add(request);
        opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
        Assert.assertEquals(opportunisticContainerContext.getOutstandingOpReqs().size(), 1);
    }

    /*
     Resource Request - {
        Location != ANY
        No of Container = 0
     }
     */
    @Test
    public void testAddToOutstandingReqsWithZeroContainer() {
        ResourceRequest request = getResourceRequest("resource", 0);
        createOutstandingOpReqs(request, getResource());
        Mockito.doReturn(outstandingOpReqs)
                .when(opportunisticContainerContext).getOutstandingOpReqs();
        List<ResourceRequest> resourceRequestList = new ArrayList<>();
        resourceRequestList.add(request);
        opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
        Assert.assertEquals(opportunisticContainerContext.
                getOutstandingOpReqs().size(), 1);
    }

    /*
     Resource Request - [
        {Location != ANY, No of Container = 0}
        {Location = ANY, No of Container = 0}
     ]
     */
    @Test
    public void testAddToOutstandingReqsWithZeroContainerAndMultipleSchedulerKey() {
        ResourceRequest req1 = getResourceRequest("resource", 0);
        ResourceRequest req2 = getResourceRequest(ResourceRequest.ANY, 0);
        createOutstandingOpReqs(req1, getResource());
        createOutstandingOpReqs(req2, getResource());
        Mockito.doReturn(outstandingOpReqs)
                .when(opportunisticContainerContext).getOutstandingOpReqs();
        List<ResourceRequest> resourceRequestList = new ArrayList<>();
        resourceRequestList.add(req1);
        resourceRequestList.add(req2);
        opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
        Assert.assertEquals(opportunisticContainerContext.
                getOutstandingOpReqs().size(), 1);
    }

    /*
     Resource Request - [
        {Location != ANY, No of Container = 0}
        {Location = ANY, No of Container != 0}
     ]
     */
    @Test
    public void testAddToOutstandingReqsWithMultipleSchedulerKey() {
        ResourceRequest req1 = getResourceRequest("resource", 0);
        ResourceRequest req2 = getResourceRequest(ResourceRequest.ANY, 1);
        createOutstandingOpReqs(req1, getResource());
        createOutstandingOpReqs(req2, getResource());
        Mockito.doReturn(outstandingOpReqs)
                .when(opportunisticContainerContext).getOutstandingOpReqs();
        List<ResourceRequest> resourceRequestList = new ArrayList<>();
        resourceRequestList.add(req1);
        resourceRequestList.add(req2);
        opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
        Assert.assertEquals(opportunisticContainerContext.
                getOutstandingOpReqs().size(), 1);
    }

    /*
     Resource Request - {
        Location != ANY
        No of container = 0
        Capability = NULL
     }
     */
    @Test
    public void testAddToOutstandingReqsWithZeroContainerAndNullCapability() {
        ResourceRequest request = getResourceRequestWithoutCapability();
        createOutstandingOpReqs(request, getResource());
        Mockito.doReturn(outstandingOpReqs)
                .when(opportunisticContainerContext).getOutstandingOpReqs();
        List<ResourceRequest> resourceRequestList = new ArrayList<>();
        resourceRequestList.add(request);
        opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
        Assert.assertEquals(opportunisticContainerContext.
                getOutstandingOpReqs().size(), 1);
    }

    /*
     Resource Request - {
        Location != ANY
        No of container = 0
        Req map is NULL
     }
     */
    @Test
    public void testAddToOutstandingReqsWithEmptyReqMap() {
        ResourceRequest request = getResourceRequest("resource", 0);
        Mockito.doReturn(new TreeMap<>())
                .when(opportunisticContainerContext).getContainerIdGenerator();
        List<ResourceRequest> resourceRequestList = new ArrayList<>();
        resourceRequestList.add(request);
        opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
        Assert.assertEquals(opportunisticContainerContext.
                getOutstandingOpReqs().size(), 0);
    }

    private void createOutstandingOpReqs(ResourceRequest req, Resource resource) {
        SchedulerRequestKey schedulerRequestKey = SchedulerRequestKey.create(req);
        reqMap.put(resource,
                new OpportunisticContainerAllocator.EnrichedResourceRequest(req));
        outstandingOpReqs.put(schedulerRequestKey, reqMap);
    }

    private ResourceRequest getResourceRequest(String resourceName, int numContainer) {
        return ResourceRequest.newBuilder()
                .resourceName(resourceName)
                .numContainers(numContainer)
                .allocationRequestId(1)
                .priority(Priority.newInstance(1))
                .capability(getResource())
                .build();
    }

    private ResourceRequest getResourceRequestWithoutCapability() {
        return ResourceRequest.newBuilder()
                .resourceName("resource")
                .numContainers(0)
                .allocationRequestId(1)
                .priority(Priority.newInstance(1))
                .build();
    }

    private Resource getResource() {
        return Resource.newInstance(1024, 2);
    }
}
