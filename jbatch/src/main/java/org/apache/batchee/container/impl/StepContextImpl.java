/*
 * Copyright 2012 International Business Machines Corp.
 * 
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.container.impl;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.Metric;
import javax.batch.runtime.context.StepContext;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class StepContextImpl implements StepContext {
    private String stepId = null;
    private BatchStatus batchStatus = null;
    private String exitStatus = null;
    private Object transientUserData = null;
    private Serializable persistentUserData = null;
    private Exception exception = null;
    private Timestamp starttime = null;
    private Timestamp endtime = null;

    private long stepExecID = 0;

    private Properties properties = new Properties();

    private String batchletProcessRetVal = null;


    private ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<String, Metric>();

    public StepContextImpl(String stepId) {
        this.stepId = stepId;
    }

    @Override
    public BatchStatus getBatchStatus() {
        return batchStatus;
    }

    @Override
    public Exception getException() {
        // TODO Auto-generated method stub
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    @Override
    public String getExitStatus() {
        return exitStatus;
    }

    @Override
    public String getStepName() {
        return stepId;
    }

    @Override
    public Metric[] getMetrics() {
        final Collection<Metric> values = metrics.values();
        return values.toArray(new Metric[values.size()]);
    }

    public Map<String, Metric> metricsAsMap() {
        for (final Metric.MetricType type : Metric.MetricType.values()) {
            metrics.putIfAbsent(type.name(), new MetricImpl(type, 0));
        }
        return Collections.unmodifiableMap(metrics);
    }

    public MetricImpl getMetric(MetricImpl.MetricType metricType) {
        return (MetricImpl) metrics.get(metricType.name());
    }

    public void addMetric(MetricImpl.MetricType metricType, long value) {
        metrics.putIfAbsent(metricType.name(), new MetricImpl(metricType, value));
    }

    @Override
    public Serializable getPersistentUserData() {
        return persistentUserData;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    public Object getTransientUserData() {
        return transientUserData;
    }

    @Override
    public void setExitStatus(String status) {
        this.exitStatus = status;
    }

    public void setBatchStatus(BatchStatus status) {
        this.batchStatus = status;
    }

    @Override
    public void setPersistentUserData(Serializable data) {
        persistentUserData = data;

    }

    public void setTransientUserData(Object data) {
        transientUserData = data;
    }

    @Override
    public String toString() {
        return (" stepId: " + stepId) + ", batchStatus: " + batchStatus + ", exitStatus: " + exitStatus
            + ", batchletProcessRetVal: " + batchletProcessRetVal + ", transientUserData: " + transientUserData + ", persistentUserData: " + persistentUserData;
    }

    @Override
    public long getStepExecutionId() {
        return stepExecID;
    }


    public void setStepExecutionId(long stepExecutionId) {
        stepExecID = stepExecutionId;
    }

    public void setStartTime(Timestamp startTS) {
        starttime = startTS;

    }

    public void setEndTime(Timestamp endTS) {
        endtime = endTS;

    }

    public Timestamp getStartTimeTS() {
        return starttime;
    }

    public Timestamp getEndTimeTS() {
        return endtime;
    }

    public String getBatchletProcessRetVal() {
        return batchletProcessRetVal;
    }

    public void setBatchletProcessRetVal(String batchletProcessRetVal) {
        this.batchletProcessRetVal = batchletProcessRetVal;
    }

}
