package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Calendar;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

public class JobConfProcessor extends AbstractProcessor {
	static Logger log = Logger.getLogger(JobConfProcessor.class);
	static  Pattern timePattern = Pattern.compile("(.*)?time=\"(.*?)\"(.*)?");
    static  Pattern hodPattern = Pattern.compile("(.*?)/(.*?)\\.(\\d+)\\.(.*?)\\.hodring/(.*?)");
    static  Pattern jobPattern = Pattern.compile("(.*?)job_(.*?)_conf\\.xml(.*?)");
	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter) 
	 throws Throwable
	{
		Long time = 0L;
		Random randomNumber = new Random();
		String tags = this.chunk.getTags();

		Matcher matcher = timePattern.matcher(tags);
		if (matcher.matches()) {
			time = Long.parseLong(matcher.group(2));
		}
		String capp = this.chunk.getApplication();
    	String hodID = "";
    	String jobID = "";
        matcher = hodPattern.matcher(capp);
        if(matcher.matches()) {
        	hodID=matcher.group(3);
        }
        matcher = jobPattern.matcher(capp);
        if(matcher.matches()) {
        	jobID=matcher.group(2);
        }
		ChukwaRecord record = new ChukwaRecord();
	    DocumentBuilderFactory docBuilderFactory 
		    = DocumentBuilderFactory.newInstance();
	    //ignore all comments inside the xml file
	    docBuilderFactory.setIgnoringComments(true);
	    try {
	        DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
 	        Document doc = null;
 	        String fileName = "test_"+randomNumber.nextInt();
 	        File tmp = new File(fileName);
 	        FileOutputStream out = new FileOutputStream(tmp);
 	        out.write(recordEntry.getBytes());
 	        out.close();
		    doc = builder.parse(fileName);
		    Element root = doc.getDocumentElement();
		    if (!"configuration".equals(root.getTagName()))
		        log.fatal("bad conf file: top-level element not <configuration>");
		    NodeList props = root.getChildNodes();
		
		    for (int i = 0; i < props.getLength(); i++) {
		        Node propNode = props.item(i);
		        if (!(propNode instanceof Element))
		            continue;
		        Element prop = (Element)propNode;
		        if (!"property".equals(prop.getTagName()))
		            log.warn("bad conf file: element not <property>");
		        NodeList fields = prop.getChildNodes();
		        String attr = null;
		        String value = null;
		        boolean finalParameter = false;
		        for (int j = 0; j < fields.getLength(); j++) {
		            Node fieldNode = fields.item(j);
		            if (!(fieldNode instanceof Element))
		                continue;
		            Element field = (Element)fieldNode;
		            if ("name".equals(field.getTagName()) && field.hasChildNodes())
		                attr = ((Text)field.getFirstChild()).getData().trim();
		            if ("value".equals(field.getTagName()) && field.hasChildNodes())
		                value = ((Text)field.getFirstChild()).getData();
		            if ("final".equals(field.getTagName()) && field.hasChildNodes())
		                finalParameter = "true".equals(((Text)field.getFirstChild()).getData());
		        }
		        
		        // Ignore this parameter if it has already been marked as 'final'
		        if (attr != null && value != null) {
		            record.add(attr, value);
		        }
		    }
		    buildGenericRecord(record,null, time,"JobConf");
			calendar.setTimeInMillis(time);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);			
			key.setKey(""+ calendar.getTimeInMillis() + "/" + hodID +"." + jobID + "/" + time);
				        
            output.collect(key,record);
            tmp.delete();
	    } catch(Exception e) {
	        e.printStackTrace();	
	        throw e;
	    }
	}
	
	public String getDataType() {
		return Torque.class.getName();
	}
}
