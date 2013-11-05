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
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class RestStepExecution {
    private long id;
    private String name;
    private BatchStatus batchStatus;
    private Date startTime;
    private Date endTime;
    private String exitStatus;
    private List<RestMetric> metrics = new LinkedList<RestMetric>();

    public void setId(long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setBatchStatus(BatchStatus batchStatus) {
        this.batchStatus = batchStatus;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public void setExitStatus(String exitStatus) {
        this.exitStatus = exitStatus;
    }

    public void setMetrics(List<RestMetric> metrics) {
        this.metrics = metrics;
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

    public List<RestMetric> getMetrics() {
        return metrics;
    }

    public static List<RestStepExecution> wrap(final List<StepExecution> stepExecutions) {
        final List<RestStepExecution> executions = new ArrayList<RestStepExecution>(stepExecutions.size());
        for (final StepExecution exec : stepExecutions) {
            executions.add(wrap(exec));
        }
        return executions;
    }

    private static RestStepExecution wrap(final StepExecution exec) {
        final RestStepExecution execution = new RestStepExecution();
        execution.setId(exec.getStepExecutionId());
        execution.setName(exec.getStepName());
        execution.setBatchStatus(exec.getBatchStatus());
        execution.setExitStatus(exec.getExitStatus());
        execution.setStartTime(exec.getStartTime());
        execution.setEndTime(exec.getEndTime());
        if (exec.getMetrics() != null) {
            for (final Metric m : exec.getMetrics()) {
                execution.getMetrics().add(RestMetric.wrap(m));
            }
        }
        return execution;
    }
}
