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
import javax.batch.runtime.JobExecution;
import java.util.Date;
import java.util.Properties;

public class JobExecutionImpl implements JobExecution {
    private final Properties properties;
    private final Date lastUpdatedTime;
    private final Date createTime;
    private final String exitStatus;
    private final Date endTime;
    private final Date startTime;
    private final BatchStatus batchStatus;
    private final String name;
    private final long id;

    public JobExecutionImpl(final long id, final String name, final Properties properties,
                            final BatchStatus batchStatus, final String exitStatus, final Date createTime,
                            final Date startTime, final Date endTime, final Date lastUpdatedTime) {
        this.properties = properties;
        this.lastUpdatedTime = lastUpdatedTime;
        this.createTime = createTime;
        this.exitStatus = exitStatus;
        this.endTime = endTime;
        this.startTime = startTime;
        this.batchStatus = batchStatus;
        this.name = name;
        this.id = id;
    }

    @Override
    public long getExecutionId() {
        return id;
    }

    @Override
    public String getJobName() {
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
    public Date getCreateTime() {
        return createTime;
    }

    @Override
    public Date getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    @Override
    public Properties getJobParameters() {
        return properties;
    }
}
