package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueCapacityVectorEntryInfo {
    private String resourceName;
    private String resourceValue;

    public QueueCapacityVectorEntryInfo() {
    }

    public QueueCapacityVectorEntryInfo(String resourceName, String resourceValue) {
        this.resourceName = resourceName;
        this.resourceValue = resourceValue;
    }
}
