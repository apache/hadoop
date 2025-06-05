package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public class IssueData {
    private List<FileContent> files = new ArrayList<>();

    @XmlElement(name = "file")  // To make the endpoint result less ambiguous
    public List<FileContent> getFiles() {
        return files;
    }

    public void setFiles(List<FileContent> files) { this.files = files; }
}
