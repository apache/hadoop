package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "AutoScalerInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class AutoScalerInfo {

    @XmlElement
    String minCapacity;

    @XmlElement
    String maxCapacity;

    public AutoScalerInfo() {
    } //JAXB needs this


   public AutoScalerInfo(ResourceScheduler rs) {
     CapacitySchedulerConfiguration schedConf =
         ((CapacityScheduler)rs).getConfiguration();
     minCapacity = schedConf.getRootAutoscalerMinimumCapacity();
     maxCapacity = schedConf.getRootAutoscalerMaximumCapacity();
   }

    public String getMinCapacity() {
        return minCapacity;
    }

    public String getMaxCapacity() {
        return maxCapacity;
    }
}
