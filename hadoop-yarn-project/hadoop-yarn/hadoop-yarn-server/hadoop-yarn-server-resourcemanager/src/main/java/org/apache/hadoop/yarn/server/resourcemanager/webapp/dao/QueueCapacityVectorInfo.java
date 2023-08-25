package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.QueueCapacityVectorEntryInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueCapacityVectorInfo {
    private String configuredCapacityVector;
    private List<QueueCapacityVectorEntryInfo> capacityVectorEntries;

    public QueueCapacityVectorInfo() {
    }

    public QueueCapacityVectorInfo(QueueCapacityVector queueCapacityVector) {
        this.configuredCapacityVector = queueCapacityVector.toString();
        this.capacityVectorEntries = new ArrayList<>();
        for (QueueCapacityVector.QueueCapacityVectorEntry queueCapacityVectorEntry : queueCapacityVector) {
            this.capacityVectorEntries.add(new QueueCapacityVectorEntryInfo(
                    queueCapacityVectorEntry.getResourceName(), queueCapacityVectorEntry.getResourceWithPostfix()));
        }
    }
}
