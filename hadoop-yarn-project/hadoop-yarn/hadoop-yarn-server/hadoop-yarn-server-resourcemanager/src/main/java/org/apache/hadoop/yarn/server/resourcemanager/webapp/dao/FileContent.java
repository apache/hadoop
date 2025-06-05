package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class FileContent {
    private String filename;
    private String contentType;
    private String content;

    public String getFilename() { return filename; }
    public void setFilename(String filename) { this.filename = filename; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
}
