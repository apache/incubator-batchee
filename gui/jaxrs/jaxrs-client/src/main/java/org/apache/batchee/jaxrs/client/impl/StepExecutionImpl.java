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
package org.apache.batchee.jaxrs.client.impl;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import java.io.Serializable;
import java.util.Date;

public class StepExecutionImpl implements StepExecution {
    private long id;
    private String name;
    private BatchStatus batchStatus;
    private Date startTime;
    private Date endTime;
    private String exitStatus;
    private Serializable persistentUserData;
    private Metric[] metrics;

    public StepExecutionImpl(final long id, final String name,
                             final BatchStatus batchStatus, final String exitStatus,
                             final Date startTime, final Date endTime,
                             final Serializable persistentUserData, final Metric[] metrics) {
        this.id = id;
        this.name = name;
        this.batchStatus = batchStatus;
        this.startTime = startTime;
        this.endTime = endTime;
        this.exitStatus = exitStatus;
        this.persistentUserData = persistentUserData;
        this.metrics = metrics;
    }

    @Override
    public long getStepExecutionId() {
        return id;
    }

    @Override
    public String getStepName() {
        return name;
    }

    @Override
    public BatchStatus getBatchStatus() {
        return batchStatus;
    }

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public Date getEndTime() {
        return endTime;
    }

    @Override
    public String getExitStatus() {
        return exitStatus;
    }

    @Override
    public Serializable getPersistentUserData() {
        return persistentUserData; // always ull when used from jaxrs client
    }

    @Override
    public Metric[] getMetrics() {
        return metrics;
    }
}
