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

import org.apache.batchee.container.impl.JobInstanceImpl;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.List;

@Entity
@NamedQueries({
    @NamedQuery(name = JobInstanceEntity.Queries.COUNT_BY_NAME, query = "select count(j) from JobInstanceEntity j where j.name = :name"),
    @NamedQuery(name = JobInstanceEntity.Queries.FIND_FROM_EXECUTION, query = "select j from JobInstanceEntity j inner join j.executions e where e.executionId = :executionId"),
    @NamedQuery(name = JobInstanceEntity.Queries.FIND_EXTERNALS, query = "select j from JobInstanceEntity j where j.name not like :pattern"),
    @NamedQuery(name = JobInstanceEntity.Queries.FIND_JOBNAMES, query = "select distinct(j.name) from JobInstanceEntity j where j.name not like :pattern"),
    @NamedQuery(name = JobInstanceEntity.Queries.FIND_BY_NAME, query = "select j from JobInstanceEntity j where j.name = :name"),
    @NamedQuery(name = JobInstanceEntity.Queries.DELETE_BY_INSTANCE_ID, query = "delete from JobInstanceEntity e where e.jobInstanceId = :instanceId"),
    @NamedQuery(
        name = JobInstanceEntity.Queries.DELETE_BY_DATE,
        query = "delete from JobInstanceEntity e where (select max(x.endTime) from JobExecutionEntity x where x.instance.jobInstanceId = e.jobInstanceId) < :date")
})
@Table(name=JobInstanceEntity.TABLE_NAME)
public class JobInstanceEntity {
    public interface Queries {
        String COUNT_BY_NAME = "org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity.countByName";
        String FIND_BY_NAME = "org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity.findByName";
        String FIND_EXTERNALS = "org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity.findExternals";
        String FIND_JOBNAMES = "org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity.findJobNames";
        String FIND_FROM_EXECUTION = "org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity.findByExecution";
        String DELETE_BY_INSTANCE_ID = "org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity.deleteFromInstanceId";
        String DELETE_BY_DATE = "org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity.deleteByDate";
    }

    public static final String TABLE_NAME = "BATCH_JOBINSTANCE";

    @Id
    @GeneratedValue
    private long jobInstanceId;

    private String name;

    @Lob
    private String jobXml;

    @Enumerated(EnumType.STRING)
    private BatchStatus batchStatus;

    private String step;
    private String exitStatus;
    private String restartOn;
    private long latestExecution;

    @OneToMany(mappedBy = "instance")
    private List<JobExecutionEntity> executions;

    public long getJobInstanceId() {
        return jobInstanceId;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setJobXml(final String jobXml) {
        this.jobXml = jobXml;
    }

    public String getJobXml() {
        return jobXml;
    }

    public String getStep() {
        return step;
    }

    public void setStep(final String step) {
        this.step = step;
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

    public String getRestartOn() {
        return restartOn;
    }

    public void setRestartOn(final String restartOn) {
        this.restartOn = restartOn;
    }

    public long getLatestExecution() {
        return latestExecution;
    }

    public void setLatestExecution(final long latestExecution) {
        this.latestExecution = latestExecution;
    }

    public List<JobExecutionEntity> getExecutions() {
        return executions;
    }

    public JobInstance toJobInstance() {
        final JobInstanceImpl jobInstance = new JobInstanceImpl(jobInstanceId, jobXml);
        jobInstance.setJobName(name);
        return jobInstance;
    }
}
