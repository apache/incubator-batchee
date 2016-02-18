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
package org.apache.batchee.container.services.persistence.jpa.domain;

import javax.batch.runtime.BatchStatus;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Entity
@NamedQueries({
    @NamedQuery(name = JobExecutionEntity.Queries.MOST_RECENT,
                query =  "SELECT e FROM JobExecutionEntity e WHERE e.instance.jobInstanceId = :instanceId ORDER BY e.createTime DESC"),
    @NamedQuery(name = JobExecutionEntity.Queries.FIND_BY_INSTANCE, query =  "SELECT e FROM JobExecutionEntity e WHERE e.instance.jobInstanceId = :instanceId"),
    @NamedQuery(name = JobExecutionEntity.Queries.DELETE_BY_INSTANCE_ID, query =  "delete from JobExecutionEntity e where e.instance.jobInstanceId = :instanceId"),
    @NamedQuery(name = JobExecutionEntity.Queries.DELETE_BY_DATE, query =  "delete from JobExecutionEntity e where e.endTime < :date"),
    @NamedQuery(name = JobExecutionEntity.Queries.FIND_RUNNING, query =  "SELECT e FROM JobExecutionEntity e WHERE e.batchStatus in :statuses and e.instance.name = :name")
})
@Table(name=JobExecutionEntity.TABLE_NAME)
public class JobExecutionEntity {
    public static interface Queries {
        String MOST_RECENT = "org.apache.batchee.container.services.persistence.jpa.domain.JobExecutionEntity.mostRecent";
        String FIND_BY_INSTANCE = "org.apache.batchee.container.services.persistence.jpa.domain.JobExecutionEntity.findByInstance";
        String FIND_RUNNING = "org.apache.batchee.container.services.persistence.jpa.domain.JobExecutionEntity.findRunning";
        String DELETE_BY_INSTANCE_ID = "org.apache.batchee.container.services.persistence.jpa.domain.JobExecutionEntity.deleteByInstanceId";
        String DELETE_BY_DATE = "org.apache.batchee.container.services.persistence.jpa.domain.JobExecutionEntity.deleteByDate";

        List<BatchStatus> RUNNING_STATUSES = Arrays.asList(BatchStatus.STARTED, BatchStatus.STARTING, BatchStatus.STOPPING);
    }

    public static final String TABLE_NAME = "BATCH_JOBEXECUTION";

    @Id
    @GeneratedValue
    private long executionId;

    private Timestamp createTime;

    private Timestamp startTime;

    private Timestamp endTime;

    private Timestamp updateTime;

    @Enumerated(EnumType.STRING)
    private BatchStatus batchStatus;

    private String exitStatus;

    @Lob
    private String jobProperties;

    @ManyToOne
    private JobInstanceEntity instance;

    public long getExecutionId() {
        return executionId;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(final Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(final Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(final Timestamp endTime) {
        this.endTime = endTime;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(final Timestamp updateTime) {
        this.updateTime = updateTime;
    }

    public BatchStatus getBatchStatus() {
        return batchStatus;
    }

    public void setBatchStatus(final BatchStatus batchStatus) {
        this.batchStatus = batchStatus;
    }

    public String getExitStatus() {
        return exitStatus;
    }

    public void setExitStatus(final String exitStatus) {
        this.exitStatus = exitStatus;
    }

    public Properties getJobProperties() {
        return PropertyHelper.stringToProperties(jobProperties);
    }

    public void setJobProperties(final Properties jobProperties) {
        this.jobProperties = PropertyHelper.propertiesToString(jobProperties);
    }

    public JobInstanceEntity getInstance() {
        return instance;
    }

    public void setInstance(final JobInstanceEntity instance) {
        this.instance = instance;
    }
}
