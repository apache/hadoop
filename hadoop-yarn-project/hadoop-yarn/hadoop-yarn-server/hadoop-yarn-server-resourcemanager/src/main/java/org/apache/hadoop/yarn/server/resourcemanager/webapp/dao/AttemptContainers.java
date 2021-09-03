package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;

@XmlRootElement(name = "attempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AttemptContainers {

  @XmlAttribute
  private String id;

  private ContainerInfo am;
  private ArrayList<ContainerInfo> container = new ArrayList<>();

  public AttemptContainers() {
  }// JAXB needs this

  public AttemptContainers(ApplicationAttemptId attemptId){
    this.id = attemptId.toString();
  }

  public ArrayList<ContainerInfo> getContainer() {
    return container;
  }

  public ContainerInfo getAm() {
    return am;
  }

  public void add(String containerId, String host) {
    container.add(new ContainerInfo(containerId, host));
  }

  public void addAm(String containerId, String host) {
    am = new ContainerInfo(containerId, host);
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  public static class ContainerInfo {

    private String id;
    private String host;

    public ContainerInfo() {
    }// JAXB needs this

    public ContainerInfo(String id, String host) {
      this.id = id;
      this.host = host;
    }

    public String getId() {
      return id;
    }

    public String getHost() {
      return host;
    }
  }
}