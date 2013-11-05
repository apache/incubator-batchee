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
package org.apache.batchee.container.impl.jobinstance;

import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.impl.JobExecutionImpl;
import org.apache.batchee.container.navigator.ModelNavigator;
import org.apache.batchee.container.proxy.ListenerFactory;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.jaxb.JSLJob;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import java.io.Closeable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;

public class RuntimeJobExecution {
    private ModelNavigator<JSLJob> jobNavigator = null;
    private JobInstance jobInstance;
    private long executionId;
    private String restartOn;
    private JobContextImpl jobContext = null;
    private ListenerFactory listenerFactory;
    private InternalJobExecution operatorJobExecution = null;
    private Integer partitionInstance = null;
    private Collection<Closeable> releasables = new ArrayList<Closeable>();

    public RuntimeJobExecution(final JobInstance jobInstance, final long executionId) {
        this.jobInstance = jobInstance;
        this.executionId = executionId;
        this.operatorJobExecution = new JobExecutionImpl(executionId, jobInstance.getInstanceId());
    }


	/*
     * Non-spec'd methods (not on the interface, but maybe we should
	 * put on a second interface).
	 */

    public void prepareForExecution(final JobContextImpl jobContext, final String restartOn) {
        this.jobContext = jobContext;
        this.jobNavigator = jobContext.getNavigator();
        jobContext.setExecutionId(executionId);
        jobContext.setInstanceId(jobInstance.getInstanceId());
        this.restartOn = restartOn;
        operatorJobExecution.setJobContext(jobContext);
    }

    public void prepareForExecution(final JobContextImpl jobContext) {
        prepareForExecution(jobContext, null);
    }

    public void setRestartOn(final String restartOn) {
        this.restartOn = restartOn;
    }

    public long getExecutionId() {
        return executionId;
    }

    public long getInstanceId() {
        return jobInstance.getInstanceId();
    }

    public JobInstance getJobInstance() {
        return jobInstance;
    }

    public ModelNavigator<JSLJob> getJobNavigator() {
        return jobNavigator;
    }

    public JobContextImpl getJobContext() {
        return jobContext;
    }

    public String getRestartOn() {
        return restartOn;
    }

    public ListenerFactory getListenerFactory() {
        return listenerFactory;
    }

    public void setListenerFactory(final ListenerFactory listenerFactory) {
        this.listenerFactory = listenerFactory;
    }

    public InternalJobExecution getJobOperatorJobExecution() {
        return operatorJobExecution;
    }

    public BatchStatus getBatchStatus() {
        return this.jobContext.getBatchStatus();
    }

    public String getExitStatus() {
        return this.jobContext.getExitStatus();
    }

    public void setBatchStatus(final String status) {
        operatorJobExecution.setBatchStatus(status);
    }

    public void setCreateTime(final Timestamp ts) {
        operatorJobExecution.setCreateTime(ts);
    }

    public void setEndTime(final Timestamp ts) {
        operatorJobExecution.setEndTime(ts);
    }

    public void setExitStatus(final String status) {
        //exitStatus = status;
        operatorJobExecution.setExitStatus(status);

    }

    public void setLastUpdateTime(final Timestamp ts) {
        operatorJobExecution.setLastUpdateTime(ts);
    }

    public void setStartTime(final Timestamp ts) {
        operatorJobExecution.setStartTime(ts);
    }

    public void setJobParameters(final Properties jProps) {
        operatorJobExecution.setJobParameters(jProps);
    }

    public Properties getJobParameters() {
        return operatorJobExecution.getJobParameters();
    }

    public Date getStartTime() {
        return operatorJobExecution.getStartTime();
    }

    public Date getEndTime() {
        return operatorJobExecution.getEndTime();
    }

    public Date getLastUpdatedTime() {
        return operatorJobExecution.getLastUpdatedTime();
    }

    public Date getCreateTime() {
        return operatorJobExecution.getCreateTime();
    }

    @Override
    public String toString() {
        return " executionId: " + executionId + " restartOn: " + restartOn + "\n-----------------------\n" + "jobInstance: \n   " + jobInstance;
    }

    public Integer getPartitionInstance() {
        return partitionInstance;
    }

    public void setPartitionInstance(final Integer partitionInstance) {
        this.partitionInstance = partitionInstance;
    }

    public Collection<Closeable> getReleasables() {
        return releasables;
    }

    public synchronized void addReleasable(final Closeable releasable) {
        releasables.add(releasable);
    }
}
