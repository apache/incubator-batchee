/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.jaxrs.common;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RestJobExecution {
    private long id;
    private String name;
    private BatchStatus batchStatus;
    private Date startTime;
    private Date endTime;
    private String exitStatus;
    private Date createTime;
    private Date lastUpdatedTime;
    private RestProperties jobParameters;

    public RestJobExecution() {
        // no-op
    }

    public void setId(final long id) {
        this.id = id;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setBatchStatus(final BatchStatus batchStatus) {
        this.batchStatus = batchStatus;
    }

    public void setStartTime(final Date startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(final Date endTime) {
        this.endTime = endTime;
    }

    public void setExitStatus(final String exitStatus) {
        this.exitStatus = exitStatus;
    }

    public void setCreateTime(final Date createTime) {
        this.createTime = createTime;
    }

    public void setLastUpdatedTime(final Date lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public void setJobParameters(final RestProperties jobParameters) {
        this.jobParameters = jobParameters;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BatchStatus getBatchStatus() {
        return batchStatus;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public String getExitStatus() {
        return exitStatus;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public RestProperties getJobParameters() {
        return jobParameters;
    }

    public static List<RestJobExecution> wrap(final List<JobExecution> jobExecutions) {
        final List<RestJobExecution> executions = new ArrayList<RestJobExecution>(jobExecutions.size());
        for (final JobExecution exec : jobExecutions) {
            executions.add(wrap(exec));
        }
        return executions;
    }

    public static RestJobExecution wrap(final JobExecution jobExecution) {
        final RestJobExecution execution = new RestJobExecution();
        execution.setId(jobExecution.getExecutionId());
        execution.setName(jobExecution.getJobName());
        execution.setBatchStatus(jobExecution.getBatchStatus());
        execution.setStartTime(jobExecution.getStartTime());
        execution.setEndTime(jobExecution.getEndTime());
        execution.setExitStatus(jobExecution.getExitStatus());
        execution.setCreateTime(jobExecution.getCreateTime());
        execution.setLastUpdatedTime(jobExecution.getLastUpdatedTime());
        execution.setJobParameters(RestProperties.wrap(jobExecution.getJobParameters()));
        return execution;
    }
}
