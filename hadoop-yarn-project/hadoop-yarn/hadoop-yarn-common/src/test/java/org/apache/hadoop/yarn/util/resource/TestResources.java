package org.apache.hadoop.yarn.util.resource;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;

public class TestResources {
    
    @Test    
     public void GpuResourcesAllocated()
     {
        
        Resource clusterResource = Resource.newInstance(0, 0, 0);

        // For lhs == rhs
        Resource lhs = Resource.newInstance(2, 2, 8, 0xFF);
        Resource rhs = Resource.newInstance(1, 1, 2, 3);
       
        Resource ret = Resources.subtract(lhs, rhs);
        Assert.assertTrue(ret.equalsWithGPUAttribute(Resource.newInstance(1, 1, 6, 0xFC)));
        
        Assert.assertTrue(Resources.fitsIn(rhs, lhs));
             
        long allcatedGPU = Resources.allocateGPUs(rhs, lhs);
        Assert.assertEquals(allcatedGPU, 3);
        
        ret = Resources.add(ret, rhs);
        Assert.assertTrue(ret.equalsWithGPUAttribute(lhs));
        
        lhs = Resource.newInstance(2, 2, 4, 0x33);
        rhs = Resource.newInstance(1, 1, 4, 0x33);
        
        ret = Resources.subtract(lhs, rhs);
        Assert.assertTrue(Resources.fitsIn(rhs, lhs));
        
        Assert.assertTrue(ret.equalsWithGPUAttribute(Resource.newInstance(1, 1, 0, 0)));
            
        ret = Resources.add(ret, rhs);
        Assert.assertTrue(ret.equalsWithGPUAttribute(lhs));
        
        allcatedGPU = Resources.allocateGPUs(rhs, lhs);
        Assert.assertEquals(allcatedGPU, 0x33);
        
        lhs = Resource.newInstance(2, 2, 4, 0x33);
        rhs = Resource.newInstance(1, 1, 2, 0);
        
        allcatedGPU = Resources.allocateGPUs(rhs, lhs);
        Assert.assertEquals(allcatedGPU, 0x30);
     }   
}
    