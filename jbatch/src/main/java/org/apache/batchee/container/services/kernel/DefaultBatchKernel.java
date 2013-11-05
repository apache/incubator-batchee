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
package org.apache.batchee.container.services.kernel;

import org.apache.batchee.container.ThreadRootController;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.impl.jobinstance.JobExecutionHelper;
import org.apache.batchee.container.impl.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.BatchKernelService;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.util.BatchFlowInSplitWorkUnit;
import org.apache.batchee.container.util.BatchPartitionWorkUnit;
import org.apache.batchee.container.util.BatchWorkUnit;
import org.apache.batchee.container.util.FlowInSplitBuilderConfig;
import org.apache.batchee.container.util.PartitionsBuilderConfig;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.spi.BatchThreadPoolService;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionNotMostRecentException;
import javax.batch.operations.JobExecutionNotRunningException;
import javax.batch.operations.JobRestartException;
import javax.batch.operations.JobStartException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.JobInstance;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultBatchKernel implements BatchKernelService {
    private final Map<Long, ThreadRootController> executionId2jobControllerMap = new ConcurrentHashMap<Long, ThreadRootController>();
    private final Set<Long> instanceIdExecutingSet = new HashSet<Long>();

    private final BatchThreadPoolService executorService;
    private final PersistenceManagerService persistenceService;

    public DefaultBatchKernel() {
        executorService = ServicesManager.service(BatchThreadPoolService.class);
        persistenceService = ServicesManager.service(PersistenceManagerService.class);
    }

    @Override
    public void init(final Properties pgcConfig) throws BatchContainerServiceException {
        // no-op
    }

    @Override
    public InternalJobExecution startJob(final String jobXML, final Properties jobParameters) throws JobStartException {
        final RuntimeJobExecution jobExecution = JobExecutionHelper.startJob(jobXML, jobParameters);

        // TODO - register with status manager

        final BatchWorkUnit batchWork = new BatchWorkUnit(this, jobExecution);
        registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

        executorService.executeTask(batchWork, null);

        return jobExecution.getJobOperatorJobExecution();
    }

    @Override
    public void stopJob(final long executionId) throws NoSuchJobExecutionException, JobExecutionNotRunningException {

        final ThreadRootController controller = this.executionId2jobControllerMap.get(executionId);
        if (controller == null) {
            throw new JobExecutionNotRunningException("JobExecution with execution id of " + executionId + "is not running.");
        }
        controller.stop();
    }

    @Override
    public InternalJobExecution restartJob(final long executionId, final Properties jobOverrideProps) throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {
        final RuntimeJobExecution jobExecution = JobExecutionHelper.restartJob(executionId, jobOverrideProps);
        final BatchWorkUnit batchWork = new BatchWorkUnit(this, jobExecution);

        registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

        executorService.executeTask(batchWork, null);

        return jobExecution.getJobOperatorJobExecution();
    }

    @Override
    public void jobExecutionDone(final RuntimeJobExecution jobExecution) {
        // Remove from executionId, instanceId map,set after job is done
        this.executionId2jobControllerMap.remove(jobExecution.getExecutionId());
        this.instanceIdExecutingSet.remove(jobExecution.getInstanceId());

        for (final Closeable closeable : jobExecution.getReleasables()) { // release CDI beans for instance
            try {
                closeable.close();
            } catch (final IOException e) {
                // no-op
            }
        }

        // AJM: ah - purge jobExecution from map here and flush to DB?
        // edit: no long want a 2 tier for the jobexecution...do want it for step execution
        // renamed method to flushAndRemoveStepExecution

    }

    public InternalJobExecution getJobExecution(final long executionId) throws NoSuchJobExecutionException {
        return JobExecutionHelper.getPersistedJobOperatorJobExecution(executionId);
    }

    @Override
    public void startGeneratedJob(final BatchWorkUnit batchWork) {
        executorService.executeTask(batchWork, null);
    }

    @Override
    public int getJobInstanceCount(final String jobName) {
        return persistenceService.jobOperatorGetJobInstanceCount(jobName);
    }

    @Override
    public JobInstance getJobInstance(final long executionId) {
        return JobExecutionHelper.getJobInstance(executionId);
    }


    /**
     * Build a list of batch work units and set them up in STARTING state but don't start them yet.
     */

    @Override
    public List<BatchPartitionWorkUnit> buildNewParallelPartitions(final PartitionsBuilderConfig config)
        throws JobRestartException, JobStartException {

        final List<JSLJob> jobModels = config.getJobModels();
        final Properties[] partitionPropertiesArray = config.getPartitionProperties();
        final List<BatchPartitionWorkUnit> batchWorkUnits = new ArrayList<BatchPartitionWorkUnit>(jobModels.size());

        int instance = 0;
        for (final JSLJob parallelJob : jobModels) {
            final Properties partitionProps = (partitionPropertiesArray == null) ? null : partitionPropertiesArray[instance];
            final RuntimeJobExecution jobExecution = JobExecutionHelper.startPartition(parallelJob, partitionProps);
            jobExecution.setPartitionInstance(instance);

            final BatchPartitionWorkUnit batchWork = new BatchPartitionWorkUnit(this, jobExecution, config);

            registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

            batchWorkUnits.add(batchWork);
            instance++;
        }

        return batchWorkUnits;
    }

    @Override
    public List<BatchPartitionWorkUnit> buildOnRestartParallelPartitions(final PartitionsBuilderConfig config) throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException {

        final List<JSLJob> jobModels = config.getJobModels();
        final Properties[] partitionProperties = config.getPartitionProperties();
        final List<BatchPartitionWorkUnit> batchWorkUnits = new ArrayList<BatchPartitionWorkUnit>(jobModels.size());

        //for now let always use a Properties array. We can add some more convenience methods later for null properties and what not

        int instance = 0;
        for (final JSLJob parallelJob : jobModels) {

            final Properties partitionProps = (partitionProperties == null) ? null : partitionProperties[instance];

            try {
                final long execId = getMostRecentExecutionId(parallelJob);
                final RuntimeJobExecution jobExecution;
                try {
                    jobExecution = JobExecutionHelper.restartPartition(execId, parallelJob, partitionProps);
                    jobExecution.setPartitionInstance(instance);
                } catch (final NoSuchJobExecutionException e) {
                    throw new IllegalStateException("Caught NoSuchJobExecutionException but this is an internal JobExecution so this shouldn't have happened: execId =" + execId, e);
                }

                final BatchPartitionWorkUnit batchWork = new BatchPartitionWorkUnit(this, jobExecution, config);
                registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());

                batchWorkUnits.add(batchWork);
            } catch (final JobExecutionAlreadyCompleteException e) {
                // no-op
            }

            instance++;
        }

        return batchWorkUnits;
    }

    @Override
    public void restartGeneratedJob(final BatchWorkUnit batchWork) throws JobRestartException {
        executorService.executeTask(batchWork, null);
    }

    @Override
    public BatchFlowInSplitWorkUnit buildNewFlowInSplitWorkUnit(final FlowInSplitBuilderConfig config) {
        final JSLJob parallelJob = config.getJobModel();

        final RuntimeFlowInSplitExecution execution = JobExecutionHelper.startFlowInSplit(parallelJob);
        final BatchFlowInSplitWorkUnit batchWork = new BatchFlowInSplitWorkUnit(this, execution, config);

        registerCurrentInstanceAndExecution(execution, batchWork.getController());
        return batchWork;
    }

    private long getMostRecentExecutionId(final JSLJob jobModel) {

        //There can only be one instance associated with a subjob's id since it is generated from an unique
        //job instance id. So there should be no way to directly start a subjob with particular
        final List<Long> instanceIds = persistenceService.jobOperatorGetJobInstanceIds(jobModel.getId(), 0, 2);

        // Maybe we should blow up on '0' too?
        if (instanceIds.size() > 1) {
            throw new IllegalStateException("Found " + instanceIds.size() + " entries for instance id = " + jobModel.getId() + ", which should not have happened.  Blowing up.");
        }

        final List<InternalJobExecution> partitionExecs = persistenceService.jobOperatorGetJobExecutions(instanceIds.get(0));

        Long execId = Long.MIN_VALUE;
        for (final InternalJobExecution partitionExec : partitionExecs) {
            if (partitionExec.getExecutionId() > execId) {
                execId = partitionExec.getExecutionId();
            }
        }
        return execId;
    }

    @Override
    public BatchFlowInSplitWorkUnit buildOnRestartFlowInSplitWorkUnit(final FlowInSplitBuilderConfig config)
        throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException {

        final JSLJob jobModel = config.getJobModel();
        final long execId = getMostRecentExecutionId(jobModel);
        final RuntimeFlowInSplitExecution jobExecution;
        try {
            jobExecution = JobExecutionHelper.restartFlowInSplit(execId, jobModel);
        } catch (final NoSuchJobExecutionException e) {
            throw new IllegalStateException("Caught NoSuchJobExecutionException but this is an internal JobExecution so this shouldn't have happened: execId =" + execId, e);
        }

        final BatchFlowInSplitWorkUnit batchWork = new BatchFlowInSplitWorkUnit(this, jobExecution, config);

        registerCurrentInstanceAndExecution(jobExecution, batchWork.getController());
        return batchWork;
    }

    private void registerCurrentInstanceAndExecution(final RuntimeJobExecution jobExecution, final ThreadRootController controller) {
        final long execId = jobExecution.getExecutionId();
        final long instanceId = jobExecution.getInstanceId();
        final String errorPrefix = "Tried to execute with Job executionId = " + execId + " and instanceId = " + instanceId + " ";
        if (executionId2jobControllerMap.get(execId) != null) {
            throw new IllegalStateException(errorPrefix + "but executionId is already currently executing.");
        } else if (instanceIdExecutingSet.contains(instanceId)) {
            throw new IllegalStateException(errorPrefix + "but another execution with this instanceId is already currently executing.");
        } else {
            instanceIdExecutingSet.add(instanceId);
            executionId2jobControllerMap.put(jobExecution.getExecutionId(), controller);
        }
    }

    @Override
    public boolean isExecutionRunning(final long executionId) {
        return executionId2jobControllerMap.containsKey(executionId);
    }
}
