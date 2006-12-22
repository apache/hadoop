/*
 * AbstractMetricsContext.java
 *
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

package org.apache.hadoop.metrics.spi;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Updater;

/**
 * The main class of the Service Provider Interface.  This class should be
 * extended in order to integrate the Metrics API with a specific metrics
 * client library. <p/>
 *
 * This class implements the internal table of metric data, and the timer
 * on which data is to be sent to the metrics system.  Subclasses must
 * override the abstract <code>emitRecord</code> method in order to transmit
 * the data. <p/>
 */
public abstract class AbstractMetricsContext implements MetricsContext {
    
    private int period = MetricsContext.DEFAULT_PERIOD;
    private Timer timer = null;
    
    //private Set<Updater> updaters = new HashSet<Updater>(1);
    private Set updaters = new HashSet(1);
    private boolean isMonitoring = false;
    
    private ContextFactory factory = null;
    private String contextName = null;
    
    //static class TagMap extends TreeMap<String,Object> {
    static class TagMap extends TreeMap {
		private static final long serialVersionUID = 3546309335061952993L;
		TagMap() {
            super();
        }
        TagMap(TagMap orig) {
            super(orig);
        }
    }
    //static class MetricMap extends TreeMap<String,Number> {}
    static class MetricMap extends TreeMap {
		private static final long serialVersionUID = -7495051861141631609L;
    }
            
    //static class RecordMap extends HashMap<TagMap,MetricMap> {}
    static class RecordMap extends HashMap {
		private static final long serialVersionUID = 259835619700264611L;
    }
    
    //private Map<String,RecordMap> bufferedData = new HashMap<String,RecordMap>();
    private Map bufferedData = new HashMap();
    

    /**
     * Creates a new instance of AbstractMetricsContext
     */
    protected AbstractMetricsContext() {
    }
    
    /**
     * Initializes the context.
     */
    public void init(String contextName, ContextFactory factory) 
    {
        this.contextName = contextName;
        this.factory = factory;
    }
    
    /**
     * Convenience method for subclasses to access factory attributes.
     */
    protected String getAttribute(String attributeName) {
        String factoryAttribute = contextName + "." + attributeName;
        return (String) factory.getAttribute(factoryAttribute);  
    }
    
    /**
     * Returns an attribute-value map derived from the factory attributes
     * by finding all factory attributes that begin with 
     * <i>contextName</i>.<i>tableName</i>.  The returned map consists of
     * those attributes with the contextName and tableName stripped off.
     */
    //protected Map<String,String> getAttributeTable(String tableName) {
    protected Map getAttributeTable(String tableName) {
        String prefix = contextName + "." + tableName + ".";
        //Map<String,String> result = new HashMap<String,String>();
        //for (String attributeName : factory.getAttributeNames()) {
        Map result = new HashMap();
        String[] attributeNames = factory.getAttributeNames();
        for (int i = 0; i < attributeNames.length; i++) {
            String attributeName = attributeNames[i];
            if (attributeName.startsWith(prefix)) {
                String name = attributeName.substring(prefix.length());
                String value = (String) factory.getAttribute(attributeName);
                result.put(name, value);
            }
        }
        return result;
    }
    
    /**
     * Returns the context name.
     */
    public String getContextName() {
        return contextName;
    }
    
    /**
     * Returns the factory by which this context was created.
     */
    public ContextFactory getContextFactory() {
        return factory;
    }
    
    /**
     * Starts or restarts monitoring, the emitting of metrics records.
     */
    public synchronized void startMonitoring()
    throws IOException {
        if (!isMonitoring) {
            startTimer();
            isMonitoring = true;
        }
    }
    
    /**
     * Stops monitoring.  This does not free buffered data. 
     * @see #close()
     */
    public synchronized void stopMonitoring() {
        if (isMonitoring) {
            stopTimer();
            isMonitoring = false;
        }
    }
    
    /**
     * Returns true if monitoring is currently in progress.
     */
    public boolean isMonitoring() {
        return isMonitoring;
    }
    
    /**
     * Stops monitoring and frees buffered data, returning this
     * object to its initial state.  
     */
    public synchronized void close() {
        stopMonitoring();
        clearUpdaters();
    } 
    
    /**
     * Creates a new AbstractMetricsRecord instance with the given <code>recordName</code>.
     * Throws an exception if the metrics implementation is configured with a fixed
     * set of record names and <code>recordName</code> is not in that set.
     * 
     * @param recordName the name of the record
     * @throws MetricsException if recordName conflicts with configuration data
     */
    public final synchronized MetricsRecord createRecord(String recordName) {
        if (bufferedData.get(recordName) == null) {
            bufferedData.put(recordName, new RecordMap());
        }
        return newRecord(recordName);
    }
    
    /**
     * Subclasses should override this if they subclass MetricsRecordImpl.
     * @param recordName the name of the record
     * @return newly created instance of MetricsRecordImpl or subclass
     */
    protected MetricsRecordImpl newRecord(String recordName) {
    	return new MetricsRecordImpl(recordName, this);
    }
    
    /**
     * Registers a callback to be called at time intervals determined by
     * the configuration.
     *
     * @param updater object to be run periodically; it should update
     * some metrics records 
     */
    public synchronized void registerUpdater(final Updater updater) {
        if (!updaters.contains(updater)) {
            updaters.add(updater);
        }
    }
    
    /**
     * Removes a callback, if it exists.
     *
     * @param updater object to be removed from the callback list
     */
    public synchronized void unregisterUpdater(Updater updater) {
        updaters.remove(updater);
    }
    
    private synchronized void clearUpdaters() {
        updaters.clear();
    }
    
    /**
     * Starts timer if it is not already started
     */
    private synchronized void startTimer() {
         if (timer == null) {
            timer = new Timer();
            TimerTask task = new TimerTask() {
                public void run() {
                	try {
                		timerEvent();
                	}
                	catch (IOException ioe) {
                		ioe.printStackTrace();
                	}
                }
            };
            long millis = period * 1000;
            timer.scheduleAtFixedRate(task, millis, millis);
         }
    }
    
    /**
     * Stops timer if it is running
     */
    private synchronized void stopTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }
    
    /**
     * Timer callback.
     */
    private synchronized void timerEvent() throws IOException {
        if (isMonitoring) {
            // Run all the registered updates
            // for (Updater updater : updaters) {
            Iterator it = updaters.iterator();
            while (it.hasNext()) {
                Updater updater = (Updater) it.next();
                try {
                    updater.doUpdates(this);
                }
                catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
            
            // Emit the records
            //for (String recordName : bufferedData.keySet()) {
            Iterator recordIt = bufferedData.keySet().iterator();
            while (recordIt.hasNext()) {
                String recordName = (String) recordIt.next();
                RecordMap recordMap = (RecordMap) bufferedData.get(recordName);
                synchronized (recordMap) {
                    //for (TagMap tagMap : recordMap.keySet()) {
                    Iterator tagIt = recordMap.keySet().iterator();
                    while (tagIt.hasNext()) {
                        TagMap tagMap = (TagMap) tagIt.next();
                        MetricMap metricMap = (MetricMap) recordMap.get(tagMap);
                        OutputRecord outRec = new OutputRecord(tagMap, metricMap);
                        emitRecord(contextName, recordName, outRec);
                    }
                }
            }
            flush();
        }
    }
    
    /**
     * Sends a record to the metrics system.
     */
    protected abstract void emitRecord(String contextName, String recordName, 
                                       OutputRecord outRec) throws IOException;
    
    /**
     * Called each period after all records have been emitted, this method does nothing.
     * Subclasses may override it in order to perform some kind of flush.
     */
    protected void flush() throws IOException {
    }
    
    /**
     * Called by MetricsRecordImpl.update().  Creates or updates a row in
     * the internal table of metric data.
     */
    protected void update(MetricsRecordImpl record) {
        String recordName = record.getRecordName();
        TagMap tagTable = record.getTagTable();
        //Map<String,MetricValue> metricUpdates = record.getMetricTable();
        Map metricUpdates = record.getMetricTable();
        
        RecordMap recordMap = (RecordMap) bufferedData.get(recordName);
        synchronized (recordMap) {
            MetricMap metricMap = (MetricMap) recordMap.get(tagTable);
            if (metricMap == null) {
                metricMap = new MetricMap();
                TagMap tagMap = new TagMap(tagTable); // clone tags
                recordMap.put(tagMap, metricMap);
            }
            //for (String metricName : metricUpdates.keySet()) {
            Iterator metricIt = metricUpdates.keySet().iterator();
            while (metricIt.hasNext()) {
                String metricName = (String) metricIt.next();
                MetricValue updateValue = (MetricValue) metricUpdates.get(metricName);
                Number updateNumber = updateValue.getNumber();
                Number currentNumber = (Number) metricMap.get(metricName);
                if (currentNumber == null || updateValue.isAbsolute()) {
                    metricMap.put(metricName, updateNumber);
                }
                else {
                    Number newNumber = sum(updateNumber, currentNumber);
                    metricMap.put(metricName, newNumber);
                }
            }
        }
    }
    
    /**
     * Adds two numbers, coercing the second to the type of the first.
     *
     */
    private Number sum(Number a, Number b) {
        if (a instanceof Integer) {
            return new Integer(a.intValue() + b.intValue());
        }
        else if (a instanceof Float) {
            return new Float(a.floatValue() + b.floatValue());
        }
        else if (a instanceof Short) {
            return new Short((short)(a.shortValue() + b.shortValue()));
        }
        else if (a instanceof Byte) {
            return new Byte((byte)(a.byteValue() + b.byteValue()));
        }
        else {
            // should never happen
            throw new MetricsException("Invalid number type");
        }
            
    }
    
    /**
     * Called by MetricsRecordImpl.remove().  Removes any matching row in
     * the internal table of metric data.  A row matches if it has the same
     * tag names and tag values.
     */    
    protected void remove(MetricsRecordImpl record) {
        String recordName = record.getRecordName();
        TagMap tagTable = record.getTagTable();
        
        RecordMap recordMap = (RecordMap) bufferedData.get(recordName);
        synchronized (recordMap) {
            recordMap.remove(tagTable);
        }
    }
    
    /**
     * Returns the timer period.
     */
    public int getPeriod() {
        return period;
    }
    
    /**
     * Sets the timer period
     */
    protected void setPeriod(int period) {
        this.period = period;
    }
}
