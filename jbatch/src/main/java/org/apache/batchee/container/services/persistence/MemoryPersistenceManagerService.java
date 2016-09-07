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
package org.apache.batchee.container.services.persistence;

import org.apache.batchee.container.impl.JobExecutionImpl;
import org.apache.batchee.container.impl.JobInstanceImpl;
import org.apache.batchee.container.impl.MetricImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.StepExecutionImpl;
import org.apache.batchee.container.impl.controller.PartitionedStepBuilder;
import org.apache.batchee.container.impl.controller.chunk.CheckpointData;
import org.apache.batchee.container.impl.controller.chunk.CheckpointDataKey;
import org.apache.batchee.container.impl.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.container.status.StepStatus;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryPersistenceManagerService implements PersistenceManagerService {
    private static final Collection<BatchStatus> RUNNING_STATUSES = new CopyOnWriteArrayList<BatchStatus>() {{
        add(BatchStatus.STARTED);
        add(BatchStatus.STARTING);
        add(BatchStatus.STOPPING);
    }};

    protected static interface Structures extends Serializable {
        static class JobInstanceData implements Structures {
            protected JobInstanceImpl instance;
            protected JobStatus status;
            protected final List<ExecutionInstanceData> executions = new LinkedList<ExecutionInstanceData>();
            protected final Collection<CheckpointDataKey> checkpoints = new LinkedList<CheckpointDataKey>();
        }

        static class ExecutionInstanceData implements Structures {
            protected final List<StepExecution> stepExecutions = new LinkedList<StepExecution>();
            protected JobExecutionImpl execution;
        }

        static class StepExecutionInstanceData implements Structures {
            protected ExecutionInstanceData jobExec;
            protected StepExecutionImpl execution;
            protected StepStatus status;
        }
    }

    static class Data {
        protected final AtomicLong lastCleanedJobInstanceId = new AtomicLong(0);
        protected final AtomicLong jobInstanceIdGenerator = new AtomicLong();
        protected final AtomicLong executionInstanceIdGenerator = new AtomicLong();
        protected final AtomicLong stepExecutionIdGenerator = new AtomicLong();

        protected final Map<CheckpointDataKey, CheckpointData> checkpointData = new ConcurrentHashMap<CheckpointDataKey, CheckpointData>();
        protected final Map<Long, Structures.JobInstanceData> jobInstanceData = new ConcurrentHashMap<Long, Structures.JobInstanceData>();
        protected final Map<Long, Structures.ExecutionInstanceData> executionInstanceData = new ConcurrentHashMap<Long, Structures.ExecutionInstanceData>();
        protected final Map<Long, Structures.StepExecutionInstanceData> stepExecutionInstanceData = new ConcurrentHashMap<Long, Structures.StepExecutionInstanceData>();
    }

    private static final Data GLOBAL_DATA = new Data();

    private Data data;
    private int maxSize;

    // an extension point to be able to use whatever map you want, a distributed/filtered/size-limited one for instance
    protected Data newData() {
        return new Data();
    }

    @Override
    public void init(final Properties batchConfig) {
        if ("true".equalsIgnoreCase(batchConfig.getProperty("persistence.memory.global", "false"))) {
            data = GLOBAL_DATA;
        } else {
            data = newData();
        }
        maxSize = Integer.parseInt(batchConfig.getProperty("persistence.memory.max-jobs-instances", "1000"));
    }

    @Override
    public int jobOperatorGetJobInstanceCount(final String jobName) {
        int i = 0;
        for (final Structures.JobInstanceData jobInstanceData : data.jobInstanceData.values()) {
            if (jobInstanceData.instance.getJobName().equals(jobName)) {
                i++;
            }
        }
        return i;
    }

    @Override
    public Set<String> getJobNames() {
        Set<String> jobNames = new HashSet<String>();
        for (final Structures.JobInstanceData jobInstanceData : data.jobInstanceData.values()) {
            if (jobInstanceData.instance.getJobName() != null && !jobInstanceData.instance.getJobName().startsWith(PartitionedStepBuilder.JOB_ID_SEPARATOR)) {
                jobNames.add(jobInstanceData.instance.getJobName());
            }
        }
        return jobNames;
    }

    @Override
    public List<Long> jobOperatorGetJobInstanceIds(final String jobName, final int start, final int count) {
        final List<Long> out = new LinkedList<Long>();
        for (final Structures.JobInstanceData jobInstanceData : data.jobInstanceData.values()) {
            if (jobInstanceData.instance.getJobName() != null && jobInstanceData.instance.getJobName().equals(jobName)) {
                out.add(jobInstanceData.instance.getInstanceId());
            }
        }

        // sorting could be optimized a bit but is it necessary for this impl?
        Collections.sort(out);
        // Collections.reverse(out);

        if (out.size() > 0) {
            try {
                return out.subList(start, start + count);
            } catch (final IndexOutOfBoundsException oobEx) {
                return out.subList(start, out.size());
            }
        }

        return out;
    }

    @Override
    public Timestamp jobOperatorQueryJobExecutionTimestamp(final long key, final TimestampType timestampType) {
        return null; // avoid infinite loops
    }

    @Override
    public String jobOperatorQueryJobExecutionBatchStatus(final long key) {
        return null; // avoid infinite loops
    }

    @Override
    public String jobOperatorQueryJobExecutionExitStatus(final long key) {
        return null; // avoid infinite loops
    }

    @Override
    public List<StepExecution> getStepExecutionsForJobExecution(final long execid) {
        final Structures.ExecutionInstanceData executionInstanceData = data.executionInstanceData.get(execid);
        if (executionInstanceData == null) {
            return Collections.emptyList();
        }

        synchronized (executionInstanceData.stepExecutions) {
            return executionInstanceData.stepExecutions;
        }
    }

    @Override
    public void updateBatchStatusOnly(final long executionId, final BatchStatus batchStatus, final Timestamp timestamp) {
        final Structures.ExecutionInstanceData toUpdate = data.executionInstanceData.get(executionId);
        toUpdate.execution.setBatchStatus(batchStatus.name());
        toUpdate.execution.setLastUpdateTime(timestamp);
    }

    @Override
    public void markJobStarted(final long key, final Timestamp startTS) {
        final Structures.ExecutionInstanceData toUpdate = data.executionInstanceData.get(key);
        toUpdate.execution.setBatchStatus(BatchStatus.STARTED.name());
        toUpdate.execution.setLastUpdateTime(startTS);
        toUpdate.execution.setStartTime(startTS);
    }

    @Override
    public void updateWithFinalExecutionStatusesAndTimestamps(final long key, final BatchStatus batchStatus, final String exitStatus, final Timestamp updatets) {
        final Structures.ExecutionInstanceData toUpdate = data.executionInstanceData.get(key);
        if (batchStatus != null) {
            toUpdate.execution.setBatchStatus(batchStatus.name());
        } else {
            toUpdate.execution.setBatchStatus(null);
        }
        toUpdate.execution.setExitStatus(exitStatus);
        toUpdate.execution.setLastUpdateTime(updatets);
        toUpdate.execution.setEndTime(updatets);
    }

    @Override
    public InternalJobExecution jobOperatorGetJobExecution(final long jobExecutionId) {
        final Structures.ExecutionInstanceData executionInstanceData = data.executionInstanceData.get(jobExecutionId);
        if (executionInstanceData == null) {
            return null;
        }
        return executionInstanceData.execution;
    }

    @Override
    public Properties getParameters(final long executionId) throws NoSuchJobExecutionException {
        return data.executionInstanceData.get(executionId).execution.getJobParameters();
    }

    @Override
    public List<InternalJobExecution> jobOperatorGetJobExecutions(final long jobInstanceId) {
        final List<InternalJobExecution> list = new LinkedList<InternalJobExecution>();
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(jobInstanceId);
        if (jobInstanceData == null) {
            return list;
        }
        synchronized (jobInstanceData.executions) {
            for (final Structures.ExecutionInstanceData executionInstanceData : jobInstanceData.executions) {
                list.add(executionInstanceData.execution);
            }
        }
        return list;
    }

    @Override
    public Set<Long> jobOperatorGetRunningExecutions(final String jobName) {
        final Set<Long> set = new HashSet<Long>();

        for (final Structures.JobInstanceData instanceData : data.jobInstanceData.values()) {
            if (instanceData.instance.getJobName().equals(jobName)) {
                synchronized (instanceData.executions) {
                    for (final Structures.ExecutionInstanceData executionInstanceData : instanceData.executions) {
                        if (RUNNING_STATUSES.contains(executionInstanceData.execution.getBatchStatus())) {
                            set.add(executionInstanceData.execution.getExecutionId());
                        }
                    }
                }
            }
        }

        return set;
    }

    @Override
    public JobStatus getJobStatusFromExecution(final long executionId) {
        final Structures.ExecutionInstanceData executionInstanceData = data.executionInstanceData.get(executionId);
        if (executionInstanceData == null) {
            return null;
        }

        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(executionInstanceData.execution.getInstanceId());
        if (jobInstanceData == null) {
            return null;
        }
        return jobInstanceData.status;
    }

    @Override
    public long getJobInstanceIdByExecutionId(final long executionId) throws NoSuchJobExecutionException {
        final Structures.ExecutionInstanceData executionInstanceData = data.executionInstanceData.get(executionId);
        if (executionInstanceData == null) {
            throw new NoSuchJobExecutionException("Execution #" + executionId);
        }
        return executionInstanceData.execution.getInstanceId();
    }

    @Override
    public JobInstance createJobInstance(final String name, final String jobXml) {
        final JobInstanceImpl jobInstance = new JobInstanceImpl(data.jobInstanceIdGenerator.getAndIncrement(), jobXml);
        jobInstance.setJobName(name);

        final Structures.JobInstanceData jobInstanceData = new Structures.JobInstanceData();
        jobInstanceData.instance = jobInstance;
        if (maxSize > 0 && data.jobInstanceData.size() >= maxSize) {
            synchronized (this) {
                while (data.jobInstanceData.size() >= maxSize) {
                    cleanUp(data.lastCleanedJobInstanceId.getAndIncrement());
                }
            }
        }
        data.jobInstanceData.put(jobInstance.getInstanceId(), jobInstanceData);

        return jobInstance;
    }

    @Override
    public RuntimeJobExecution createJobExecution(final JobInstance jobInstance, final Properties jobParameters, final BatchStatus batchStatus) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());

        final Structures.ExecutionInstanceData executionInstanceData = createRuntimeJobExecutionEntry(jobInstance, jobParameters, batchStatus, now);
        executionInstanceData.execution.setJobName(jobInstance.getJobName());

        final RuntimeJobExecution jobExecution = new RuntimeJobExecution(jobInstance, executionInstanceData.execution.getExecutionId(), this);
        jobExecution.setBatchStatus(batchStatus.name());
        jobExecution.setCreateTime(now);
        jobExecution.setLastUpdateTime(now);

        return jobExecution;
    }

    private Structures.ExecutionInstanceData createRuntimeJobExecutionEntry(final JobInstance jobInstance, final Properties jobParameters,
                                                                            final BatchStatus batchStatus, final Timestamp now) {
        final Structures.ExecutionInstanceData executionInstanceData = new Structures.ExecutionInstanceData();
        final long id = data.executionInstanceIdGenerator.getAndIncrement();
        executionInstanceData.execution = new JobExecutionImpl(id, jobInstance.getInstanceId(), this);
        executionInstanceData.execution.setExecutionId(id);
        executionInstanceData.execution.setInstanceId(jobInstance.getInstanceId());
        executionInstanceData.execution.setBatchStatus(batchStatus.name());
        executionInstanceData.execution.setCreateTime(now);
        executionInstanceData.execution.setLastUpdateTime(now);
        executionInstanceData.execution.setJobParameters(jobParameters);

        data.executionInstanceData.put(id, executionInstanceData);
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(jobInstance.getInstanceId());
        synchronized (jobInstanceData.executions) {
            jobInstanceData.executions.add(executionInstanceData);
        }
        return executionInstanceData;
    }

    @Override
    public StepExecutionImpl createStepExecution(final long rootJobExecId, final StepContextImpl stepContext) {
        final String batchStatus = stepContext.getBatchStatus() == null ? BatchStatus.STARTING.name() : stepContext.getBatchStatus().name();
        final String exitStatus = stepContext.getExitStatus();
        final String stepName = stepContext.getStepName();

        long readCount = 0;
        long writeCount = 0;
        long commitCount = 0;
        long rollbackCount = 0;
        long readSkipCount = 0;
        long processSkipCount = 0;
        long filterCount = 0;
        long writeSkipCount = 0;
        Timestamp startTime = stepContext.getStartTimeTS();
        Timestamp endTime = stepContext.getEndTimeTS();

        final Metric[] metrics = stepContext.getMetrics();
        for (final Metric metric : metrics) {
            if (metric.getType().equals(MetricImpl.MetricType.READ_COUNT)) {
                readCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_COUNT)) {
                writeCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.PROCESS_SKIP_COUNT)) {
                processSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.COMMIT_COUNT)) {
                commitCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.ROLLBACK_COUNT)) {
                rollbackCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.READ_SKIP_COUNT)) {
                readSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.FILTER_COUNT)) {
                filterCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_SKIP_COUNT)) {
                writeSkipCount = metric.getValue();
            }
        }
        final Serializable persistentData = stepContext.getPersistentUserData();

        return createStepExecution(rootJobExecId, batchStatus, exitStatus, stepName, readCount,
            writeCount, commitCount, rollbackCount, readSkipCount, processSkipCount, filterCount, writeSkipCount, startTime,
            endTime, persistentData);
    }

//CHECKSTYLE:OFF
    private StepExecutionImpl createStepExecution(final long rootJobExecId, final String batchStatus, final String exitStatus, final String stepName,
                                                  final long readCount, final long writeCount, final long commitCount, final long rollbackCount,
                                                  final long readSkipCount, final long processSkipCount, final long filterCount, final long writeSkipCount,
                                                  final Timestamp startTime, final Timestamp endTime, final Serializable persistentData) {
//CHECKSTYLE:ON

        final StepExecutionImpl stepExecution = new StepExecutionImpl(rootJobExecId, data.stepExecutionIdGenerator.getAndIncrement());
        stepExecution.setStepName(stepName);
        final Structures.ExecutionInstanceData executionInstanceData = data.executionInstanceData.get(rootJobExecId);
        if (executionInstanceData == null) {
            return null;
        }
        synchronized (executionInstanceData.stepExecutions) {
            executionInstanceData.stepExecutions.add(stepExecution);
        }

        final Structures.StepExecutionInstanceData stepExecutionInstanceData = new Structures.StepExecutionInstanceData();
        stepExecutionInstanceData.execution = stepExecution;
        updateStepExecutionInstanceData(executionInstanceData, batchStatus, exitStatus, stepName,
            readCount, writeCount, commitCount, rollbackCount,
            readSkipCount, processSkipCount, filterCount, writeSkipCount,
            startTime, endTime, persistentData,
            stepExecutionInstanceData);
        data.stepExecutionInstanceData.put(stepExecution.getStepExecutionId(), stepExecutionInstanceData);

        return stepExecution;
    }

//CHECKSTYLE:OFF
    private void updateStepExecutionInstanceData(final Structures.ExecutionInstanceData exec,
                                                 final String batchStatus,final  String exitStatus, final String stepName,
                                                 final long readCount, final long writeCount, final long commitCount, final long rollbackCount,
                                                 final long readSkipCount, final long processSkipCount, final long filterCount, final long writeSkipCount,
                                                 final Timestamp startTime, final Timestamp endTime,
                                                 final Serializable persistentData,
                                                 final Structures.StepExecutionInstanceData stepExecutionInstanceData) {
//CHECKSTYLE:ON
        stepExecutionInstanceData.jobExec = exec;
        stepExecutionInstanceData.execution.setExitStatus(exitStatus);
        stepExecutionInstanceData.execution.setBatchStatus(BatchStatus.valueOf(batchStatus));
        stepExecutionInstanceData.execution.setRollbackCount(rollbackCount);
        stepExecutionInstanceData.execution.setStepName(stepName);
        stepExecutionInstanceData.execution.setReadCount(readCount);
        stepExecutionInstanceData.execution.setWriteCount(writeCount);
        stepExecutionInstanceData.execution.setCommitCount(commitCount);
        stepExecutionInstanceData.execution.setRollbackCount(rollbackCount);
        stepExecutionInstanceData.execution.setReadSkipCount(readSkipCount);
        stepExecutionInstanceData.execution.setProcessSkipCount(processSkipCount);
        stepExecutionInstanceData.execution.setWriteSkipCount(writeSkipCount);
        stepExecutionInstanceData.execution.setFilterCount(filterCount);
        stepExecutionInstanceData.execution.setWriteSkipCount(writeSkipCount);
        stepExecutionInstanceData.execution.setStartTime(startTime);
        stepExecutionInstanceData.execution.setEndTime(endTime);
        stepExecutionInstanceData.execution.setPersistentUserData(persistentData);
    }

    @Override
    public void updateStepExecution(final long jobExecId, final StepContextImpl stepContext) {
        final long stepExecutionId = stepContext.getStepInternalExecID();
        final String batchStatus = stepContext.getBatchStatus() == null ? BatchStatus.STARTING.name() : stepContext.getBatchStatus().name();
        final String exitStatus = stepContext.getExitStatus();
        final String stepName = stepContext.getStepName();

        long readCount = 0;
        long writeCount = 0;
        long commitCount = 0;
        long rollbackCount = 0;
        long readSkipCount = 0;
        long processSkipCount = 0;
        long filterCount = 0;
        long writeSkipCount = 0;
        Timestamp startTime = stepContext.getStartTimeTS();
        Timestamp endTime = stepContext.getEndTimeTS();

        final Metric[] metrics = stepContext.getMetrics();
        for (final Metric metric : metrics) {
            if (metric.getType().equals(MetricImpl.MetricType.READ_COUNT)) {
                readCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_COUNT)) {
                writeCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.PROCESS_SKIP_COUNT)) {
                processSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.COMMIT_COUNT)) {
                commitCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.ROLLBACK_COUNT)) {
                rollbackCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.READ_SKIP_COUNT)) {
                readSkipCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.FILTER_COUNT)) {
                filterCount = metric.getValue();
            } else if (metric.getType().equals(MetricImpl.MetricType.WRITE_SKIP_COUNT)) {
                writeSkipCount = metric.getValue();
            }
        }
        final Serializable persistentData = stepContext.getPersistentUserData();

        updateStepExecution(stepExecutionId, jobExecId, batchStatus, exitStatus, stepName, readCount,
            writeCount, commitCount, rollbackCount, readSkipCount, processSkipCount, filterCount,
            writeSkipCount, startTime, endTime, persistentData);
    }

//CHECKSTYLE:OFF
    private void updateStepExecution(final long stepExecutionId, final long jobExecId, final String batchStatus, final String exitStatus, final String stepName,
                                     final long readCount, final long writeCount, final long commitCount, final long rollbackCount,
                                     final long readSkipCount, final long processSkipCount, final long filterCount, final long writeSkipCount,
                                     final Timestamp startTime, final Timestamp endTime, final Serializable persistentData) {
//CHECKSTYLE:ON
        final Structures.ExecutionInstanceData executionInstanceData = data.executionInstanceData.get(jobExecId);
        if (executionInstanceData == null) {
            return;
        }

        synchronized (executionInstanceData.stepExecutions) {
            for (final StepExecution execution : executionInstanceData.stepExecutions) {
                if (execution.getStepExecutionId() == stepExecutionId) {
                    updateStepExecutionInstanceData(executionInstanceData, batchStatus, exitStatus, stepName,
                        readCount, writeCount, commitCount, rollbackCount,
                        readSkipCount, processSkipCount, filterCount, writeSkipCount,
                        startTime, endTime, persistentData,
                        data.stepExecutionInstanceData.get(stepExecutionId));
                    break;
                }
            }
        }
    }

    @Override
    public JobStatus createJobStatus(final long jobInstanceId) {
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(jobInstanceId);
        jobInstanceData.status = new JobStatus(jobInstanceId);
        return jobInstanceData.status;
    }

    @Override
    public JobStatus getJobStatus(final long instanceId) {
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(instanceId);
        if (jobInstanceData == null) {
            return null;
        }
        return jobInstanceData.status;
    }

    @Override
    public void updateJobStatus(final long instanceId, final JobStatus jobStatus) {
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(instanceId);
        if (jobInstanceData != null) {
            jobInstanceData.status = jobStatus;
        }
    }

    @Override
    public StepStatus createStepStatus(final long stepExecId) {
        final StepStatus stepStatus = new StepStatus(stepExecId);
        data.stepExecutionInstanceData.get(stepExecId).status = stepStatus;
        return stepStatus;
    }

    @Override
    public StepStatus getStepStatus(final long instanceId, final String stepName) {
        final Map<Date, StepStatus> statusMap = new TreeMap<Date, StepStatus>(ReverseDateComparator.INSTANCE);
        for (final Structures.StepExecutionInstanceData stepExecutionInstanceData : data.stepExecutionInstanceData.values()) {
            if (stepExecutionInstanceData.jobExec.execution.getInstanceId() == instanceId
                    && stepExecutionInstanceData.execution.getStepName().equals(stepName)
                    && stepExecutionInstanceData.status != null) {
                statusMap.put(stepExecutionInstanceData.jobExec.execution.getCreateTime(), stepExecutionInstanceData.status);
            }
        }
        if (statusMap.isEmpty()) {
            return null;
        }
        return statusMap.values().iterator().next();
    }

    @Override
    public void updateStepStatus(final long stepExecutionId, final StepStatus stepStatus) {
        final Structures.StepExecutionInstanceData stepExecutionInstanceData = data.stepExecutionInstanceData.get(stepExecutionId);
        if (stepExecutionInstanceData != null) {
            stepExecutionInstanceData.status = stepStatus;
        }
    }

    @Override
    public void setCheckpointData(final CheckpointDataKey key, final CheckpointData value) {
        data.checkpointData.put(key, value);
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(key.getJobInstanceId());
        synchronized (jobInstanceData.checkpoints) {
            jobInstanceData.checkpoints.add(key);
        }
    }

    @Override
    public CheckpointData getCheckpointData(final CheckpointDataKey key) {
        return data.checkpointData.get(key);
    }

    @Override
    public long getMostRecentExecutionId(final long jobInstanceId) {
        final Map<Date, Structures.ExecutionInstanceData> filter = new TreeMap<Date, Structures.ExecutionInstanceData>(ReverseDateComparator.INSTANCE);
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.get(jobInstanceId);
        synchronized (jobInstanceData.executions) {
            for (final Structures.ExecutionInstanceData exec : jobInstanceData.executions) {
                if (exec.execution.getInstanceId() == jobInstanceId) {
                    filter.put(exec.execution.getCreateTime(), exec);
                }
            }
        }
        if (filter.isEmpty()) {
            return -1;
        }
        return filter.values().iterator().next().execution.getExecutionId();
    }

    @Override
    public JobInstance createSubJobInstance(final String name) {
        return createJobInstance(name, null);
    }

    @Override
    public RuntimeFlowInSplitExecution createFlowInSplitExecution(final JobInstance jobInstance, final BatchStatus batchStatus) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        final Structures.ExecutionInstanceData executionInstanceData = createRuntimeJobExecutionEntry(jobInstance, null, batchStatus, now);
        final RuntimeFlowInSplitExecution flowExecution = new RuntimeFlowInSplitExecution(jobInstance, executionInstanceData.execution.getExecutionId(), this);
        flowExecution.setBatchStatus(batchStatus.name());
        flowExecution.setCreateTime(now);
        flowExecution.setLastUpdateTime(now);
        return flowExecution;
    }

    @Override
    public StepExecution getStepExecutionByStepExecutionId(final long stepExecId) {
        return data.stepExecutionInstanceData.get(stepExecId).execution;
    }

    @Override
    public void cleanUp(final long instanceId) {
        final Structures.JobInstanceData jobInstanceData = data.jobInstanceData.remove(instanceId);
        if (jobInstanceData == null) {
            return;
        }

        synchronized (jobInstanceData.executions) {
            for (final Structures.ExecutionInstanceData executionInstanceData : jobInstanceData.executions) {
                data.executionInstanceData.remove(executionInstanceData.execution.getExecutionId());
                synchronized (executionInstanceData.stepExecutions) {
                    for (final StepExecution stepExecution : executionInstanceData.stepExecutions) {
                        data.stepExecutionInstanceData.remove(stepExecution.getStepExecutionId());
                    }
                }
            }
        }
        synchronized (jobInstanceData.checkpoints) {
            for (final CheckpointDataKey key : jobInstanceData.checkpoints) {
                data.checkpointData.remove(key);
            }
        }
    }

    @Override
    public void cleanUp(final Date until) {
        final Collection<Long> instanceIdToRemove = new ArrayList<Long>();
        for (final Map.Entry<Long, Structures.JobInstanceData> entry : data.jobInstanceData.entrySet()) {
            boolean match = true;
            for (final Structures.ExecutionInstanceData exec : entry.getValue().executions) {
                if (exec.execution.getEndTime() == null || exec.execution.getEndTime().after(until)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                instanceIdToRemove.add(entry.getKey());
            }
        }

        for (final Long id : instanceIdToRemove) {
            cleanUp(id);
        }
    }

    private static class ReverseDateComparator implements Comparator<Date> {
        public static final ReverseDateComparator INSTANCE = new ReverseDateComparator();

        @Override
        public int compare(final Date o1, final Date o2) {
            return o2.compareTo(o1);
        }
    }

    @Override
    public String toString() {
        return getClass().getName();
    }
}
