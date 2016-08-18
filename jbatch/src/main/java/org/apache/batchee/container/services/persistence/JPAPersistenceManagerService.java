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

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.exception.PersistenceException;
import org.apache.batchee.container.impl.JobExecutionImpl;
import org.apache.batchee.container.impl.JobInstanceImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.StepExecutionImpl;
import org.apache.batchee.container.impl.controller.PartitionedStepBuilder;
import org.apache.batchee.container.impl.controller.chunk.CheckpointData;
import org.apache.batchee.container.impl.controller.chunk.CheckpointDataKey;
import org.apache.batchee.container.impl.controller.chunk.PersistentDataWrapper;
import org.apache.batchee.container.impl.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.persistence.jpa.EntityManagerProvider;
import org.apache.batchee.container.services.persistence.jpa.TransactionProvider;
import org.apache.batchee.container.services.persistence.jpa.domain.CheckpointEntity;
import org.apache.batchee.container.services.persistence.jpa.domain.JobExecutionEntity;
import org.apache.batchee.container.services.persistence.jpa.domain.JobInstanceEntity;
import org.apache.batchee.container.services.persistence.jpa.domain.StepExecutionEntity;
import org.apache.batchee.container.services.persistence.jpa.provider.DefaultEntityManagerProvider;
import org.apache.batchee.container.services.persistence.jpa.provider.DefaultTransactionProvider;
import org.apache.batchee.container.services.persistence.jpa.provider.EEEntityManagerProvider;
import org.apache.batchee.container.services.persistence.jpa.provider.EETransactionProvider;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.container.status.StepStatus;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.TemporalType;
import javax.persistence.TypedQuery;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.batchee.container.util.Serializations.deserialize;
import static org.apache.batchee.container.util.Serializations.serialize;

public class JPAPersistenceManagerService implements PersistenceManagerService {
    private final static Logger LOGGER = Logger.getLogger(JPAPersistenceManagerService.class.getName());

    private static final String[] DELETE_ID_QUERIES = {
        StepExecutionEntity.Queries.DELETE_BY_INSTANCE_ID, CheckpointEntity.Queries.DELETE_BY_INSTANCE_ID,
        JobExecutionEntity.Queries.DELETE_BY_INSTANCE_ID, JobInstanceEntity.Queries.DELETE_BY_INSTANCE_ID
    };
    private static final String[] DELETE_DATE_QUERIES = {
        StepExecutionEntity.Queries.DELETE_BY_DATE, CheckpointEntity.Queries.DELETE_BY_DATE,
        JobInstanceEntity.Queries.DELETE_BY_DATE, JobExecutionEntity.Queries.DELETE_BY_DATE
    };

    private EntityManagerProvider emProvider;
    private TransactionProvider txProvider;

    @Override
    public void cleanUp(final long instanceId) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                for (final String query : DELETE_ID_QUERIES) {
                    em.createNamedQuery(query).setParameter("instanceId", instanceId).executeUpdate();
                }
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public void cleanUp(final Date until) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                for (final String query : DELETE_DATE_QUERIES) {
                    em.createNamedQuery(query).setParameter("date", until, TemporalType.TIMESTAMP).executeUpdate();
                }
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public StepStatus getStepStatus(final long instanceId, final String stepName) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final List<StepExecutionEntity> list = em.createNamedQuery(StepExecutionEntity.Queries.FIND_BY_INSTANCE_AND_NAME, StepExecutionEntity.class)
                .setParameter("instanceId", instanceId)
                .setParameter("step", stepName)
                .getResultList();
            if (list != null && !list.isEmpty()) {
                final StepExecutionEntity entity = list.iterator().next();
                final StepStatus status = new StepStatus(entity.getId(), entity.getStartCount());
                status.setBatchStatus(entity.getBatchStatus());
                status.setExitStatus(entity.getExitStatus());
                status.setNumPartitions(entity.getNumPartitions());
                status.setLastRunStepExecutionId(entity.getLastRunStepExecutionId());
                if (entity.getPersistentData() != null) {
                    status.setPersistentUserData(new PersistentDataWrapper(entity.getPersistentData()));
                }
                return status;
            }
            return null;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public Set<String> getJobNames() {
        Set<String> jobNames = new TreeSet<String>();
        final EntityManager em = emProvider.newEntityManager();

        try {
            final List<String> list = em.createNamedQuery(JobInstanceEntity.Queries.FIND_JOBNAMES, String.class)
                                        .setParameter("pattern", PartitionedStepBuilder.JOB_ID_SEPARATOR + "%")
                                        .getResultList();
            jobNames.addAll(list);
        } finally {
            emProvider.release(em);
        }
        return jobNames;
    }

    @Override
    public JobStatus getJobStatusFromExecution(final long executionId) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final JobInstanceEntity entity = em.createNamedQuery(JobInstanceEntity.Queries.FIND_FROM_EXECUTION, JobInstanceEntity.class)
                .setParameter("executionId", executionId)
                .getSingleResult();
            final JobStatus status = new JobStatus(entity.getJobInstanceId());
            setJobStatusData(status, entity);
            return status;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public StepExecution getStepExecutionByStepExecutionId(final long stepExecId) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final StepExecutionEntity entity = em.find(StepExecutionEntity.class, stepExecId);
            final StepExecutionImpl stepEx = new StepExecutionImpl(entity.getExecution().getExecutionId(), stepExecId);
            setStepExecutionData(entity, stepEx);
            return stepEx;
        } finally {
            emProvider.release(em);
        }
    }

    private void setStepExecutionData(final StepExecutionEntity entity, final StepExecutionImpl stepEx) {
        stepEx.setBatchStatus(entity.getBatchStatus());
        stepEx.setExitStatus(entity.getExitStatus());
        stepEx.setStepName(entity.getStepName());
        stepEx.setReadCount(entity.getRead());
        stepEx.setWriteCount(entity.getWrite());
        stepEx.setCommitCount(entity.getCommit());
        stepEx.setRollbackCount(entity.getRollback());
        stepEx.setReadSkipCount(entity.getReadSkip());
        stepEx.setProcessSkipCount(entity.getProcessSkip());
        stepEx.setFilterCount(entity.getFilter());
        stepEx.setWriteSkipCount(entity.getWriteSkip());
        stepEx.setStartTime(entity.getStartTime());
        stepEx.setEndTime(entity.getEndTime());
        try {
            stepEx.setPersistentUserData(deserialize(entity.getPersistentData()));
        } catch (final Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public List<StepExecution> getStepExecutionsForJobExecution(final long execid) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final List<StepExecutionEntity> steps = em.createNamedQuery(StepExecutionEntity.Queries.FIND_BY_EXECUTION, StepExecutionEntity.class)
                .setParameter("executionId", execid)
                .getResultList();

            if (steps == null) {
                return Collections.emptyList();
            }

            final List<StepExecution> executions = new ArrayList<StepExecution>(steps.size());
            for (final StepExecutionEntity entity : steps) {
                final StepExecutionImpl stepEx = new StepExecutionImpl(execid, entity.getId());
                setStepExecutionData(entity, stepEx);
                executions.add(stepEx);
            }

            return executions;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public void updateStepExecution(final long jobExecId, final StepContextImpl stepContext) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                final StepExecutionEntity entity = em.find(StepExecutionEntity.class, stepContext.getStepInternalExecID());
                setStepData(em, jobExecId, stepContext,
                    stepContext.metricsAsMap().get(Metric.MetricType.READ_COUNT.name()).getValue(),
                    stepContext.metricsAsMap().get(Metric.MetricType.WRITE_COUNT.name()).getValue(),
                    stepContext.metricsAsMap().get(Metric.MetricType.COMMIT_COUNT.name()).getValue(),
                    stepContext.metricsAsMap().get(Metric.MetricType.ROLLBACK_COUNT.name()).getValue(),
                    stepContext.metricsAsMap().get(Metric.MetricType.READ_SKIP_COUNT.name()).getValue(),
                    stepContext.metricsAsMap().get(Metric.MetricType.PROCESS_SKIP_COUNT.name()).getValue(),
                    stepContext.metricsAsMap().get(Metric.MetricType.FILTER_COUNT.name()).getValue(),
                    stepContext.metricsAsMap().get(Metric.MetricType.WRITE_SKIP_COUNT.name()).getValue(),
                    entity);
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public StepExecutionImpl createStepExecution(final long jobExecId, final StepContextImpl stepContext) {
        final StepExecutionEntity entity = new StepExecutionEntity();
        final EntityManager em = emProvider.newEntityManager();
        try {
            setStepData(em, jobExecId, stepContext,
                stepContext.metricsAsMap().get(Metric.MetricType.READ_COUNT.name()).getValue(),
                stepContext.metricsAsMap().get(Metric.MetricType.WRITE_COUNT.name()).getValue(),
                stepContext.metricsAsMap().get(Metric.MetricType.COMMIT_COUNT.name()).getValue(),
                stepContext.metricsAsMap().get(Metric.MetricType.ROLLBACK_COUNT.name()).getValue(),
                stepContext.metricsAsMap().get(Metric.MetricType.READ_SKIP_COUNT.name()).getValue(),
                stepContext.metricsAsMap().get(Metric.MetricType.PROCESS_SKIP_COUNT.name()).getValue(),
                stepContext.metricsAsMap().get(Metric.MetricType.FILTER_COUNT.name()).getValue(),
                stepContext.metricsAsMap().get(Metric.MetricType.WRITE_SKIP_COUNT.name()).getValue(),
                entity);

            final Object tx = txProvider.start(em);
            try {
                em.persist(entity);
                txProvider.commit(tx);

                final StepExecutionImpl stepExecution = new StepExecutionImpl(jobExecId, entity.getId());
                stepExecution.setStepName(stepContext.getStepName());
                return stepExecution;
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

//CHECKSTYLE:OFF
    private void setStepData(final EntityManager em,
                             final long jobExecId, final StepContextImpl stepContext,
                             final long readCount, final long writeCount,
                             final long commitCount, final long rollbackCount,
                             final long readSkipCount, final long processSkipCount,
                             final long filterCount, final long writeSkipCount,
                             final StepExecutionEntity entity) {
//CHECKSTYLE:ON
        entity.setExecution(em.find(JobExecutionEntity.class, jobExecId));
        entity.setBatchStatus(stepContext.getBatchStatus());
        entity.setExitStatus(stepContext.getExitStatus());
        entity.setStepName(stepContext.getStepName());
        entity.setRead(readCount);
        entity.setWrite(writeCount);
        entity.setCommit(commitCount);
        entity.setRollback(rollbackCount);
        entity.setReadSkip(readSkipCount);
        entity.setProcessSkip(processSkipCount);
        entity.setFilter(filterCount);
        entity.setWriteSkip(writeSkipCount);
        entity.setStartTime(stepContext.getStartTimeTS());
        entity.setEndTime(stepContext.getEndTimeTS());
        try {
            entity.setPersistentData(serialize(stepContext.getPersistentUserData()));
        } catch (final IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public long getJobInstanceIdByExecutionId(final long executionId) throws NoSuchJobExecutionException {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final JobExecutionEntity jobExecutionEntity = em.find(JobExecutionEntity.class, executionId);
            if (jobExecutionEntity == null) {
                throw new NoSuchJobExecutionException("Execution #" + executionId);
            }
            return jobExecutionEntity.getInstance().getJobInstanceId();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public Set<Long> jobOperatorGetRunningExecutions(final String jobName) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final List<JobExecutionEntity> list = em.createNamedQuery(JobExecutionEntity.Queries.FIND_RUNNING, JobExecutionEntity.class)
                .setParameter("name", jobName)
                .setParameter("statuses", JobExecutionEntity.Queries.RUNNING_STATUSES)
                .getResultList();

            if (list == null) {
                return Collections.emptySet();
            }

            final Set<Long> ids= new HashSet<Long>(list.size());
            for (final JobExecutionEntity entity : list) {
                ids.add(entity.getExecutionId());
            }
            return ids;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public List<InternalJobExecution> jobOperatorGetJobExecutions(final long jobInstanceId) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final List<JobExecutionEntity> list = em.createNamedQuery(JobExecutionEntity.Queries.FIND_BY_INSTANCE, JobExecutionEntity.class)
                .setParameter("instanceId", jobInstanceId).getResultList();

            if (list == null) {
                return Collections.emptyList();
            }

            final List<InternalJobExecution> result = new ArrayList<InternalJobExecution>(list.size());
            for (final JobExecutionEntity entity : list) {
                final JobExecutionImpl jobEx = new JobExecutionImpl(entity.getExecutionId(), jobInstanceId, this);
                jobEx.setCreateTime(entity.getCreateTime());
                jobEx.setStartTime(entity.getStartTime());
                jobEx.setEndTime(entity.getEndTime());
                jobEx.setLastUpdateTime(entity.getUpdateTime());
                jobEx.setBatchStatus(entity.getBatchStatus().name());
                jobEx.setExitStatus(entity.getExitStatus());
                jobEx.setJobName(entity.getInstance().getName());
                jobEx.setJobParameters(entity.getJobProperties());

                result.add(jobEx);
            }
            return result;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public void updateWithFinalExecutionStatusesAndTimestamps(final long key, final BatchStatus batchStatus, final String exitStatus, final Timestamp updatets) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                final JobExecutionEntity instance = em.find(JobExecutionEntity.class, key);
                instance.setBatchStatus(batchStatus);
                instance.setUpdateTime(updatets);
                instance.setEndTime(updatets);
                instance.setExitStatus(exitStatus);

                em.merge(instance);
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public InternalJobExecution jobOperatorGetJobExecution(final long jobExecutionId) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final JobExecutionEntity instance = em.find(JobExecutionEntity.class, jobExecutionId);

            final JobExecutionImpl jobEx = new JobExecutionImpl(jobExecutionId, instance.getInstance().getJobInstanceId(), this);
            jobEx.setCreateTime(instance.getCreateTime());
            jobEx.setStartTime(instance.getStartTime());
            jobEx.setEndTime(instance.getEndTime());
            jobEx.setJobParameters(instance.getJobProperties());
            jobEx.setLastUpdateTime(instance.getUpdateTime());
            if (instance.getBatchStatus() != null) {
                jobEx.setBatchStatus(instance.getBatchStatus().name());
            }
            jobEx.setExitStatus(instance.getExitStatus());
            jobEx.setJobName(instance.getInstance().getName());
            return jobEx;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public void markJobStarted(final long key, final Timestamp startTS) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                final JobExecutionEntity instance = em.find(JobExecutionEntity.class, key);
                instance.setBatchStatus(BatchStatus.STARTED);
                instance.setStartTime(startTS);
                instance.setUpdateTime(startTS);

                em.merge(instance);
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public Properties getParameters(final long executionId) throws NoSuchJobExecutionException {
        final EntityManager em = emProvider.newEntityManager();
        try {
            return em.find(JobExecutionEntity.class, executionId).getJobProperties();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public String jobOperatorQueryJobExecutionExitStatus(final long key) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            return em.find(JobExecutionEntity.class, key).getExitStatus();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public String jobOperatorQueryJobExecutionBatchStatus(final long key) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            return em.find(JobExecutionEntity.class, key).getBatchStatus().name();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public Timestamp jobOperatorQueryJobExecutionTimestamp(final long key, final TimestampType timetype) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final JobExecutionEntity entity = em.find(JobExecutionEntity.class, key);
            if (timetype.equals(TimestampType.CREATE)) {
                return entity.getCreateTime();
            } else if (timetype.equals(TimestampType.END)) {
                return entity.getEndTime();
            } else if (timetype.equals(TimestampType.LAST_UPDATED)) {
                return entity.getUpdateTime();
            } else if (timetype.equals(TimestampType.STARTED)) {
                return entity.getStartTime();
            } else {
                throw new IllegalArgumentException("Unexpected enum value.");
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public RuntimeFlowInSplitExecution createFlowInSplitExecution(final JobInstance jobInstance, final BatchStatus batchStatus) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final JobExecutionEntity instance = new JobExecutionEntity();
            instance.setCreateTime(new Timestamp(System.currentTimeMillis()));
            instance.setUpdateTime(instance.getCreateTime());
            instance.setBatchStatus(batchStatus);

            final Object tx = txProvider.start(em);
            try {
                instance.setInstance(em.find(JobInstanceEntity.class, jobInstance.getInstanceId()));

                em.persist(instance);
                txProvider.commit(tx);

                final RuntimeFlowInSplitExecution jobExecution = new RuntimeFlowInSplitExecution(jobInstance, instance.getExecutionId(), this);
                jobExecution.setBatchStatus(batchStatus.name());
                jobExecution.setCreateTime(instance.getCreateTime());
                jobExecution.setLastUpdateTime(instance.getCreateTime());
                return jobExecution;
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public long getMostRecentExecutionId(final long jobInstanceId) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            return em.createNamedQuery(JobExecutionEntity.Queries.MOST_RECENT, JobExecutionEntity.class)
                .setParameter("instanceId", jobInstanceId)
                .setMaxResults(1)
                .getSingleResult().getExecutionId();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public void updateBatchStatusOnly(final long executionId, final BatchStatus batchStatus, final Timestamp timestamp) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            final JobExecutionEntity instance = em.find(JobExecutionEntity.class, executionId);
            instance.setBatchStatus(batchStatus);
            instance.setUpdateTime(timestamp);

            try {
                em.merge(instance);
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public RuntimeJobExecution createJobExecution(final JobInstance jobInstance, final Properties jobParameters, final BatchStatus batchStatus) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final JobExecutionEntity execution = new JobExecutionEntity();
            execution.setJobProperties(jobParameters);
            execution.setCreateTime(new Timestamp(System.currentTimeMillis()));
            execution.setUpdateTime(execution.getCreateTime());
            execution.setBatchStatus(batchStatus);

            final Object tx = txProvider.start(em);
            try {
                execution.setInstance(em.find(JobInstanceEntity.class, jobInstance.getInstanceId()));

                em.persist(execution);
                txProvider.commit(tx);

                final RuntimeJobExecution jobExecution = new RuntimeJobExecution(jobInstance, execution.getExecutionId(), this);
                jobExecution.setBatchStatus(batchStatus.name());
                jobExecution.setCreateTime(execution.getCreateTime());
                jobExecution.setLastUpdateTime(execution.getCreateTime());
                return jobExecution;
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public JobInstance createSubJobInstance(final String name) {
        return createJobInstance(name, null);
    }

    @Override
    public List<Long> jobOperatorGetJobInstanceIds(final String jobName, final int start, final int count) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final TypedQuery<JobInstanceEntity> query;
            query = em.createNamedQuery(JobInstanceEntity.Queries.FIND_BY_NAME, JobInstanceEntity.class);
            query.setParameter("name", jobName);

            final List<JobInstanceEntity> resultList = query
                .setFirstResult(start)
                .setMaxResults(count)
                .getResultList();

            if (resultList == null) {
                return Collections.emptyList();
            }

            final List<Long> result = new ArrayList<Long>(resultList.size());
            for (final JobInstanceEntity entity : resultList) {
                result.add(entity.getJobInstanceId());
            }
            return result;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public JobInstance createJobInstance(final String name, final String jobXml) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final JobInstanceEntity instance = new JobInstanceEntity();
            instance.setName(name);
            instance.setJobXml(jobXml);

            final Object tx = txProvider.start(em);
            try {
                em.persist(instance);
                txProvider.commit(tx);

                final JobInstanceImpl jobInstance = new JobInstanceImpl(instance.getJobInstanceId(), jobXml);
                jobInstance.setJobName(name);
                return jobInstance;
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public int jobOperatorGetJobInstanceCount(final String jobName) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            return em.createNamedQuery(JobInstanceEntity.Queries.COUNT_BY_NAME, Number.class)
                .setParameter("name", jobName)
                .getSingleResult().intValue();
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public void updateStepStatus(final long stepExecutionId, final StepStatus stepStatus) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                final StepExecutionEntity entity = em.find(StepExecutionEntity.class, stepExecutionId);
                entity.setBatchStatus(stepStatus.getBatchStatus());
                entity.setExitStatus(stepStatus.getExitStatus());
                entity.setLastRunStepExecutionId(stepStatus.getLastRunStepExecutionId());
                entity.setNumPartitions(stepStatus.getNumPartitions());
                entity.setPersistentData(stepStatus.getRawPersistentUserData());
                entity.setStartCount(stepStatus.getStartCount());

                em.merge(entity);
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public StepStatus createStepStatus(final long stepExecId) {
        return new StepStatus(stepExecId); // instance already created
    }

    @Override
    public void updateJobStatus(final long instanceId, final JobStatus jobStatus) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                final JobInstanceEntity entity = em.find(JobInstanceEntity.class, instanceId);
                entity.setBatchStatus(jobStatus.getBatchStatus());
                entity.setStep(jobStatus.getCurrentStepId());
                entity.setLatestExecution(jobStatus.getLatestExecutionId());
                entity.setExitStatus(jobStatus.getExitStatus());
                entity.setRestartOn(jobStatus.getRestartOn());
                if (jobStatus.getJobInstance() != null) {
                    entity.setName(jobStatus.getJobInstance().getJobName());
                }
                em.merge(entity);

                final List<JobExecutionEntity> executions = em.createNamedQuery(JobExecutionEntity.Queries.FIND_BY_INSTANCE, JobExecutionEntity.class)
                    .setParameter("instanceId", instanceId)
                    .getResultList();
                if (executions != null) {
                    for (final JobExecutionEntity e : executions) {
                        e.setInstance(entity);
                        em.merge(e);
                    }
                }
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public JobStatus getJobStatus(final long instanceId) {
        final JobStatus status = new JobStatus(instanceId);
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                setJobStatusData(status, em.find(JobInstanceEntity.class, instanceId));
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
        return status;
    }

    private void setJobStatusData(final JobStatus status, final JobInstanceEntity entity) {
        status.setBatchStatus(entity.getBatchStatus());
        status.setCurrentStepId(entity.getStep());
        status.setLatestExecutionId(entity.getLatestExecution());
        status.setExitStatus(entity.getExitStatus());
        status.setRestartOn(entity.getRestartOn());
        status.setJobInstance(entity.toJobInstance());
    }

    @Override
    public JobStatus createJobStatus(final long jobInstanceId) {
        return new JobStatus(jobInstanceId); // instance already created
    }

    @Override
    public void setCheckpointData(final CheckpointDataKey key, final CheckpointData value) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final Object tx = txProvider.start(em);
            try {
                final List<CheckpointEntity> checkpoints = em.createNamedQuery(CheckpointEntity.Queries.FIND, CheckpointEntity.class)
                    .setParameter("jobInstanceId", key.getJobInstanceId())
                    .setParameter("stepName", key.getStepName())
                    .setParameter("type", key.getType())
                    .getResultList();

                final CheckpointEntity checkpoint;
                final boolean isNew = checkpoints == null || checkpoints.isEmpty();
                if (isNew) {
                    checkpoint = new CheckpointEntity();
                    checkpoint.setInstance(em.find(JobInstanceEntity.class, key.getJobInstanceId()));
                    checkpoint.setStepName(key.getStepName());
                    checkpoint.setType(key.getType());
                } else {
                    checkpoint = checkpoints.iterator().next();
                }

                checkpoint.setData(value.getRestartToken());

                if (isNew) {
                    em.persist(checkpoint);
                } else {
                    em.merge(checkpoint);
                }
                txProvider.commit(tx);
            } catch (final Exception e) {
                throw new BatchContainerRuntimeException(performRollback(tx, e));
            }
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public CheckpointData getCheckpointData(final CheckpointDataKey key) {
        final EntityManager em = emProvider.newEntityManager();
        try {
            final CheckpointEntity checkpoint = em.createNamedQuery(CheckpointEntity.Queries.FIND, CheckpointEntity.class)
                .setParameter("jobInstanceId", key.getJobInstanceId())
                .setParameter("stepName", key.getStepName())
                .setParameter("type", key.getType())
                .getSingleResult();

            final CheckpointData data = new CheckpointData(checkpoint.getInstance().getJobInstanceId(), checkpoint.getStepName(), checkpoint.getType());
            data.setRestartToken(checkpoint.getData());
            return data;
        } catch (final NoResultException nre) {
            return null;
        } finally {
            emProvider.release(em);
        }
    }

    @Override
    public void init(final Properties batchConfig) {
        final boolean ee = "true".equalsIgnoreCase(batchConfig.getProperty("persistence.jpa.ee", "false"));
        final String txProviderClass = batchConfig.getProperty(
            "persistence.jpa.transaction-provider", ee ? EETransactionProvider.class.getName() : DefaultTransactionProvider.class.getName());
        try {
            txProvider = TransactionProvider.class.cast(Thread.currentThread().getContextClassLoader().loadClass(txProviderClass).newInstance());
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException(e);
        }
        txProvider.init(batchConfig);

        final String providerClass = batchConfig.getProperty(
            "persistence.jpa.entity-manager-provider", ee ? EEEntityManagerProvider.class.getName() : DefaultEntityManagerProvider.class.getName());
        try {
            emProvider = EntityManagerProvider.class.cast(Thread.currentThread().getContextClassLoader().loadClass(providerClass).newInstance());
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException(e);
        }
        emProvider.init(batchConfig);
    }

    /**
     * Rollback the transaction and eventually log any exception during rollback.
     * This method ensures that an Exception during rollback will *not* hide
     * the original Exception!
     *
     * @return the original Exception
     */
    private Exception performRollback(Object tx, Exception originalException) {
        try {
            txProvider.rollback(tx, originalException);
        } catch (Exception exceptionDuringRollback) {
            LOGGER.log(Level.SEVERE, "Got an Exception while rolling back due to another Exception. Printing Rollback Exception and re-throwing original one"
                    , exceptionDuringRollback);
        }
        return originalException;
    }
}
