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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.sql.Timestamp;

@Entity
@NamedQueries({
    @NamedQuery(name = StepExecutionEntity.Queries.FIND_BY_EXECUTION, query = "select s from StepExecutionEntity s where s.execution.executionId = :executionId"),
    @NamedQuery(name = StepExecutionEntity.Queries.DELETE_BY_INSTANCE_ID, query = "delete from StepExecutionEntity e where e.execution.instance.jobInstanceId = :instanceId"),
    @NamedQuery(name = StepExecutionEntity.Queries.FIND_BY_INSTANCE_AND_NAME, query = "select se FROM StepExecutionEntity se where se.execution.instance.jobInstanceId = :instanceId and se.stepName = :step")
})
public class StepExecutionEntity {
    public static interface Queries {
        String FIND_BY_EXECUTION = "org.apache.batchee.container.services.persistence.jpa.domain.StepExecutionEntity.findByExecution";
        String FIND_BY_INSTANCE_AND_NAME = "org.apache.batchee.container.services.persistence.jpa.domain.StepExecutionEntity.findByInstanceAndName";
        String DELETE_BY_INSTANCE_ID = "org.apache.batchee.container.services.persistence.jpa.domain.StepExecutionEntity.deleteByInstanceId";
    }

    @Id
    @GeneratedValue
    private long id;

    @Enumerated(EnumType.STRING)
    private BatchStatus batchStatus;

    private String stepName;
    private long read;
    private long write;
    private long commit;
    private long rollback;
    private long readSkip;
    private long processSkip;
    private long filter;
    private long writeSkip;

    @Temporal(TemporalType.TIMESTAMP)
    private Timestamp startTime;

    @Temporal(TemporalType.TIMESTAMP)
    private Timestamp endTime;

    @Lob
    private byte[] persistentData;

    @ManyToOne
    private JobExecutionEntity execution;

    private String exitStatus;
    private int startCount;
    private Integer numPartitions;
    private long lastRunStepExecutionId;

    public long getId() {
        return id;
    }

    public JobExecutionEntity getExecution() {
        return execution;
    }

    public void setExecution(final JobExecutionEntity execution) {
        this.execution = execution;
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

    public String getStepName() {
        return stepName;
    }

    public void setStepName(final String stepName) {
        this.stepName = stepName;
    }

    public long getRead() {
        return read;
    }

    public void setRead(final long read) {
        this.read = read;
    }

    public long getWrite() {
        return write;
    }

    public void setWrite(final long write) {
        this.write = write;
    }

    public long getCommit() {
        return commit;
    }

    public void setCommit(final long commit) {
        this.commit = commit;
    }

    public long getRollback() {
        return rollback;
    }

    public void setRollback(final long rollback) {
        this.rollback = rollback;
    }

    public long getReadSkip() {
        return readSkip;
    }

    public void setReadSkip(final long readSkip) {
        this.readSkip = readSkip;
    }

    public long getProcessSkip() {
        return processSkip;
    }

    public void setProcessSkip(final long processSkip) {
        this.processSkip = processSkip;
    }

    public long getFilter() {
        return filter;
    }

    public void setFilter(final long filter) {
        this.filter = filter;
    }

    public long getWriteSkip() {
        return writeSkip;
    }

    public void setWriteSkip(final long writeSkip) {
        this.writeSkip = writeSkip;
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

    public byte[] getPersistentData() {
        return persistentData;
    }

    public void setPersistentData(final byte[] persistentData) {
        this.persistentData = persistentData;
    }

    public int getStartCount() {
        return startCount;
    }

    public void setStartCount(final int startCount) {
        this.startCount = startCount;
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(final Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public long getLastRunStepExecutionId() {
        return lastRunStepExecutionId;
    }

    public void setLastRunStepExecutionId(final long lastRunStepExecutionId) {
        this.lastRunStepExecutionId = lastRunStepExecutionId;
    }


}
