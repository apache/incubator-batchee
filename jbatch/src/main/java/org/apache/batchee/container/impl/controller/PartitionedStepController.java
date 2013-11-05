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
package org.apache.batchee.container.impl.controller;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.jsl.CloneUtility;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.PartitionAnalyzerProxy;
import org.apache.batchee.container.proxy.PartitionMapperProxy;
import org.apache.batchee.container.proxy.PartitionReducerProxy;
import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.proxy.StepListenerProxy;
import org.apache.batchee.container.util.BatchPartitionPlan;
import org.apache.batchee.container.util.BatchPartitionWorkUnit;
import org.apache.batchee.container.util.BatchWorkUnit;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.container.util.PartitionDataWrapper.PartitionEventType;
import org.apache.batchee.container.util.PartitionsBuilderConfig;
import org.apache.batchee.jaxb.Analyzer;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.JSLProperties;
import org.apache.batchee.jaxb.PartitionMapper;
import org.apache.batchee.jaxb.PartitionReducer;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;

import javax.batch.api.partition.PartitionPlan;
import javax.batch.api.partition.PartitionReducer.PartitionStatus;
import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionNotMostRecentException;
import javax.batch.operations.JobRestartException;
import javax.batch.operations.JobStartException;
import javax.batch.runtime.BatchStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PartitionedStepController extends BaseStepController {
    private static final int DEFAULT_PARTITION_INSTANCES = 1;
    private static final int DEFAULT_THREADS = 0; //0 means default to number of instances

    private PartitionPlan plan = null;

    private int partitions = DEFAULT_PARTITION_INSTANCES;
    private int threads = DEFAULT_THREADS;

    private Properties[] partitionProperties = null;

    private volatile List<BatchPartitionWorkUnit> parallelBatchWorkUnits;

    private PartitionReducerProxy partitionReducerProxy = null;

    // On invocation this will be re-primed to reflect already-completed partitions from a previous execution.
    int numPreviouslyCompleted = 0;

    private PartitionAnalyzerProxy analyzerProxy = null;

    final List<JSLJob> subJobs = new ArrayList<JSLJob>();
    protected List<StepListenerProxy> stepListeners = null;

    List<BatchPartitionWorkUnit> completedWork = new ArrayList<BatchPartitionWorkUnit>();

    BlockingQueue<BatchPartitionWorkUnit> completedWorkQueue = null;

    protected PartitionedStepController(final RuntimeJobExecution jobExecutionImpl, final Step step, StepContextImpl stepContext, long rootJobExecutionId) {
        super(jobExecutionImpl, step, stepContext, rootJobExecutionId);
    }

    @Override
    public void stop() {

        updateBatchStatus(BatchStatus.STOPPING);

        // It's possible we may try to stop a partitioned step before any
        // sub steps have been started.
        synchronized (subJobs) {

            if (parallelBatchWorkUnits != null) {
                for (BatchWorkUnit subJob : parallelBatchWorkUnits) {
                    try {
                        BATCH_KERNEL.stopJob(subJob.getJobExecutionImpl().getExecutionId());
                    } catch (Exception e) {
                        // TODO - Is this what we want to know.
                        // Blow up if it happens to force the issue.
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
    }

    private PartitionPlan generatePartitionPlan() {
        // Determine the number of partitions


        PartitionPlan plan = null;
        Integer previousNumPartitions = null;
        final PartitionMapper partitionMapper = step.getPartition().getMapper();

        //from persisted plan from previous run
        if (stepStatus.getNumPartitions() != null) {
            previousNumPartitions = stepStatus.getNumPartitions();
        }

        if (partitionMapper != null) { //from partition mapper

            final List<Property> propertyList = partitionMapper.getProperties() == null ? null
                : partitionMapper.getProperties().getPropertyList();

            // Set all the contexts associated with this controller.
            // Some of them may be null
            final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, propertyList);
            final PartitionMapperProxy partitionMapperProxy = ProxyFactory.createPartitionMapperProxy(partitionMapper.getRef(), injectionRef, stepContext, jobExecutionImpl);


            final PartitionPlan mapperPlan = partitionMapperProxy.mapPartitions();

            //Set up the new partition plan
            plan = new BatchPartitionPlan();
            plan.setPartitionsOverride(mapperPlan.getPartitionsOverride());

            //When true is specified, the partition count from the current run
            //is used and all results from past partitions are discarded.
            if (mapperPlan.getPartitionsOverride() || previousNumPartitions == null) {
                plan.setPartitions(mapperPlan.getPartitions());
            } else {
                plan.setPartitions(previousNumPartitions);
            }

            if (mapperPlan.getThreads() == 0) {
                plan.setThreads(plan.getPartitions());
            } else {
                plan.setThreads(mapperPlan.getThreads());
            }

            plan.setPartitionProperties(mapperPlan.getPartitionProperties());
        } else if (step.getPartition().getPlan() != null) { //from static partition element in jsl


            final String partitionsAttr = step.getPartition().getPlan().getPartitions();
            String threadsAttr;

            int numPartitions = Integer.MIN_VALUE;
            int numThreads;
            Properties[] partitionProps = null;

            if (partitionsAttr != null) {
                try {
                    numPartitions = Integer.parseInt(partitionsAttr);
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Could not parse partition instances value in stepId: " + step.getId()
                        + ", with instances=" + partitionsAttr, e);
                }
                partitionProps = new Properties[numPartitions];
                if (numPartitions < 1) {
                    throw new IllegalArgumentException("Partition instances value must be 1 or greater in stepId: " + step.getId()
                        + ", with instances=" + partitionsAttr);
                }
            }

            threadsAttr = step.getPartition().getPlan().getThreads();
            if (threadsAttr != null) {
                try {
                    numThreads = Integer.parseInt(threadsAttr);
                    if (numThreads == 0) {
                        numThreads = numPartitions;
                    }
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Could not parse partition threads value in stepId: " + step.getId()
                        + ", with threads=" + threadsAttr, e);
                }
                if (numThreads < 0) {
                    throw new IllegalArgumentException("Threads value must be 0 or greater in stepId: " + step.getId()
                        + ", with threads=" + threadsAttr);

                }
            } else { //default to number of partitions if threads isn't set
                numThreads = numPartitions;
            }


            if (step.getPartition().getPlan().getProperties() != null) {

                List<JSLProperties> jslProperties = step.getPartition().getPlan().getProperties();
                for (JSLProperties props : jslProperties) {
                    int targetPartition = Integer.parseInt(props.getPartition());

                    try {
                        partitionProps[targetPartition] = CloneUtility.jslPropertiesToJavaProperties(props);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        throw new BatchContainerRuntimeException("There are only " + numPartitions + " partition instances, but there are "
                            + jslProperties.size()
                            + " partition properties lists defined. Remember that partition indexing is 0 based like Java arrays.", e);
                    }
                }
            }
            plan = new BatchPartitionPlan();
            plan.setPartitions(numPartitions);
            plan.setThreads(numThreads);
            plan.setPartitionProperties(partitionProps);
            plan.setPartitionsOverride(false); //FIXME what is the default for a static plan??
        }


        // Set the other instance variables for convenience.
        this.partitions = plan.getPartitions();
        this.threads = plan.getThreads();
        this.partitionProperties = plan.getPartitionProperties();

        return plan;
    }


    @Override
    protected void invokeCoreStep() throws JobRestartException, JobStartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException {

        this.plan = this.generatePartitionPlan();

        //persist the partition plan so on restart we have the same plan to reuse
        stepStatus.setNumPartitions(plan.getPartitions());

		/* When true is specified, the partition count from the current run
         * is used and all results from past partitions are discarded. Any
		 * resource cleanup or back out of work done in the previous run is the
		 * responsibility of the application. The PartitionReducer artifact's
		 * rollbackPartitionedStep method is invoked during restart before any
		 * partitions begin processing to provide a cleanup hook.
		 */
        if (plan.getPartitionsOverride()) {
            if (this.partitionReducerProxy != null) {
                this.partitionReducerProxy.rollbackPartitionedStep();
            }
        }

        //Set up a blocking queue to pick up collector data from a partitioned thread
        if (this.analyzerProxy != null) {
            this.analyzerStatusQueue = new LinkedBlockingQueue<PartitionDataWrapper>();
        }
        this.completedWorkQueue = new LinkedBlockingQueue<BatchPartitionWorkUnit>();

        // Build all sub jobs from partitioned step
        buildSubJobBatchWorkUnits();

        // kick off the threads
        executeAndWaitForCompletion();

        // Deal with the results.
        checkCompletedWork();
    }

    private void buildSubJobBatchWorkUnits() throws JobRestartException, JobStartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException {
        synchronized (subJobs) {
            //check if we've already issued a stop
            if (jobExecutionImpl.getJobContext().getBatchStatus().equals(BatchStatus.STOPPING)) {
                return;
            }

            for (int instance = 0; instance < partitions; instance++) {
                subJobs.add(PartitionedStepBuilder.buildPartitionSubJob(jobExecutionImpl.getInstanceId(), jobExecutionImpl.getJobContext(), step, instance));
            }

            PartitionsBuilderConfig config = new PartitionsBuilderConfig(subJobs, partitionProperties, analyzerStatusQueue, completedWorkQueue, jobExecutionImpl.getExecutionId());
            // Then build all the subjobs but do not start them yet
            if (stepStatus.getStartCount() > 1 && !plan.getPartitionsOverride()) {
                parallelBatchWorkUnits = BATCH_KERNEL.buildOnRestartParallelPartitions(config);
            } else {
                parallelBatchWorkUnits = BATCH_KERNEL.buildNewParallelPartitions(config);
            }

            // NOTE:  At this point I might not have as many work units as I had partitions, since some may have already completed.
        }
    }

    private void executeAndWaitForCompletion() throws JobRestartException {

        if (jobExecutionImpl.getJobContext().getBatchStatus().equals(BatchStatus.STOPPING)) {
            return;
        }

        int numTotalForThisExecution = parallelBatchWorkUnits.size();
        this.numPreviouslyCompleted = partitions - numTotalForThisExecution;
        int numCurrentCompleted = 0;
        int numCurrentSubmitted = 0;

        //Start up to to the max num we are allowed from the num threads attribute
        for (int i = 0; i < this.threads && i < numTotalForThisExecution; i++, numCurrentSubmitted++) {
            final BatchWorkUnit workUnit = parallelBatchWorkUnits.get(i);
            if (stepStatus.getStartCount() > 1 && !plan.getPartitionsOverride()) {
                BATCH_KERNEL.restartGeneratedJob(workUnit);
            } else {
                BATCH_KERNEL.startGeneratedJob(workUnit);
            }
        }

        while (true) {
            try {
                if (analyzerProxy != null) {
                    PartitionDataWrapper dataWrapper = analyzerStatusQueue.take();
                    if (PartitionEventType.ANALYZE_COLLECTOR_DATA.equals(dataWrapper.getEventType())) {
                        analyzerProxy.analyzeCollectorData(dataWrapper.getCollectorData());
                        continue; // without being ready to submit another
                    } else if (PartitionEventType.ANALYZE_STATUS.equals(dataWrapper.getEventType())) {
                        analyzerProxy.analyzeStatus(dataWrapper.getBatchstatus(), dataWrapper.getExitStatus());
                        completedWork.add(completedWorkQueue.take());  // Shouldn't be a a long wait.
                    } else {
                        throw new IllegalStateException("Invalid partition state");
                    }
                } else {
                    // block until at least one thread has finished to
                    // submit more batch work. hold on to the finished work to look at later
                    completedWork.add(completedWorkQueue.take());
                }
            } catch (final InterruptedException e) {
                throw new BatchContainerRuntimeException(e);
            }

            numCurrentCompleted++;
            if (numCurrentCompleted < numTotalForThisExecution) {
                if (numCurrentSubmitted < numTotalForThisExecution) {
                    if (stepStatus.getStartCount() > 1) {
                        BATCH_KERNEL.startGeneratedJob(parallelBatchWorkUnits.get(numCurrentSubmitted++));
                    } else {
                        BATCH_KERNEL.restartGeneratedJob(parallelBatchWorkUnits.get(numCurrentSubmitted++));
                    }
                }
            } else {
                break;
            }
        }
    }

    private void checkCompletedWork() {
        /**
         * check the batch status of each subJob after it's done to see if we need to issue a rollback
         * start rollback if any have stopped or failed
         */
        boolean rollback = false;

        for (final BatchWorkUnit subJob : completedWork) {
            BatchStatus batchStatus = subJob.getJobExecutionImpl().getJobContext().getBatchStatus();
            if (batchStatus.equals(BatchStatus.FAILED)) {
                rollback = true;

                //Keep track of the failing status and throw an exception to propagate after the rest of the partitions are complete
                stepContext.setBatchStatus(BatchStatus.FAILED);
            }
        }

        //If rollback is false we never issued a rollback so we can issue a logicalTXSynchronizationBeforeCompletion
        //NOTE: this will get issued even in a subjob fails or stops if no logicalTXSynchronizationRollback method is provied
        //We are assuming that not providing a rollback was intentional
        if (rollback) {
            if (this.partitionReducerProxy != null) {
                this.partitionReducerProxy.rollbackPartitionedStep();
            }
            throw new BatchContainerRuntimeException("One or more partitions failed");
        } else {
            if (this.partitionReducerProxy != null) {
                this.partitionReducerProxy.beforePartitionedStepCompletion();
            }
        }
    }

    @Override
    protected void setupStepArtifacts() {
        InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, null);
        this.stepListeners = jobExecutionImpl.getListenerFactory().getStepListeners(step, injectionRef, stepContext, jobExecutionImpl);

        final Analyzer analyzer = step.getPartition().getAnalyzer();
        if (analyzer != null) {
            final List<Property> propList = analyzer.getProperties() == null ? null : analyzer.getProperties().getPropertyList();
            injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, propList);
            analyzerProxy = ProxyFactory.createPartitionAnalyzerProxy(analyzer.getRef(), injectionRef, stepContext, jobExecutionImpl);
        }

        final PartitionReducer partitionReducer = step.getPartition().getReducer();
        if (partitionReducer != null) {
            final List<Property> propList = partitionReducer.getProperties() == null ? null : partitionReducer.getProperties().getPropertyList();
            injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, propList);
            partitionReducerProxy = ProxyFactory.createPartitionReducerProxy(partitionReducer.getRef(), injectionRef, stepContext, jobExecutionImpl);
        }

    }

    @Override
    protected void invokePreStepArtifacts() {

        if (stepListeners != null) {
            for (StepListenerProxy listenerProxy : stepListeners) {
                // Call beforeStep on all the step listeners
                listenerProxy.beforeStep();
            }
        }

        // Invoke the reducer before all parallel steps start (must occur
        // before mapper as well)
        if (this.partitionReducerProxy != null) {
            this.partitionReducerProxy.beginPartitionedStep();
        }

    }

    @Override
    protected void invokePostStepArtifacts() {
        // Invoke the reducer after all parallel steps are done
        if (this.partitionReducerProxy != null) {

            if ((BatchStatus.COMPLETED).equals(stepContext.getBatchStatus())) {
                this.partitionReducerProxy.afterPartitionedStepCompletion(PartitionStatus.COMMIT);
            } else {
                this.partitionReducerProxy.afterPartitionedStepCompletion(PartitionStatus.ROLLBACK);
            }

        }

        // Called in spec'd order, e.g. Sec. 11.7
        if (stepListeners != null) {
            for (StepListenerProxy listenerProxy : stepListeners) {
                // Call afterStep on all the step listeners
                listenerProxy.afterStep();
            }
        }
    }

    @Override
    protected void sendStatusFromPartitionToAnalyzerIfPresent() {
        // Since we're already on the main thread, there will never
        // be anything to do on this thread.  It's only on the partitioned
        // threads that there is something to send back.
    }
}
