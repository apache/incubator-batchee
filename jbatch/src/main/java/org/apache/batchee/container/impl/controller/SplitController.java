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

import org.apache.batchee.container.ExecutionElementController;
import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeFlowInSplitExecution;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.BatchKernelService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.status.ExecutionStatus;
import org.apache.batchee.container.status.ExtendedBatchStatus;
import org.apache.batchee.container.status.SplitExecutionStatus;
import org.apache.batchee.container.util.BatchFlowInSplitWorkUnit;
import org.apache.batchee.container.util.BatchParallelWorkUnit;
import org.apache.batchee.container.util.FlowInSplitBuilderConfig;
import org.apache.batchee.jaxb.Flow;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.Split;

import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionNotMostRecentException;
import javax.batch.operations.JobRestartException;
import javax.batch.operations.JobStartException;
import javax.batch.operations.NoSuchJobExecutionException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class SplitController implements ExecutionElementController {
    private final static Logger logger = Logger.getLogger(SplitController.class.getName());

    private final RuntimeJobExecution jobExecution;

    private volatile List<BatchFlowInSplitWorkUnit> parallelBatchWorkUnits;

    private final BatchKernelService batchKernel;
    private final JobContextImpl jobContext;
    private final BlockingQueue<BatchFlowInSplitWorkUnit> completedWorkQueue = new LinkedBlockingQueue<BatchFlowInSplitWorkUnit>();
    private final long rootJobExecutionId;

    private final List<JSLJob> subJobs = new ArrayList<JSLJob>();

    protected Split split;

    public SplitController(RuntimeJobExecution jobExecution, Split split, long rootJobExecutionId) {
        this.jobExecution = jobExecution;
        this.jobContext = jobExecution.getJobContext();
        this.rootJobExecutionId = rootJobExecutionId;
        this.split = split;

        batchKernel = ServicesManager.service(BatchKernelService.class);
    }

    @Override
    public void stop() {

        // It's possible we may try to stop a split before any
        // sub steps have been started.
        synchronized (subJobs) {

            if (parallelBatchWorkUnits != null) {
                for (BatchParallelWorkUnit subJob : parallelBatchWorkUnits) {
                    try {
                        batchKernel.stopJob(subJob.getJobExecutionImpl().getExecutionId());
                    } catch (final Exception e) {
                        // TODO - Is this what we want to know.
                        // Blow up if it happens to force the issue.
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
    }

    @Override
    public SplitExecutionStatus execute() throws JobRestartException, JobStartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {
        // Build all sub jobs from partitioned step
        buildSubJobBatchWorkUnits();

        // kick off the threads
        executeWorkUnits();

        // Deal with the results.
        return waitForCompletionAndAggregateStatus();
    }

    /**
     * Note we restart all flows.  There is no concept of "the flow completed".   It is only steps
     * within the flows that may have already completed and so may not have needed to be rerun.
     */
    private void buildSubJobBatchWorkUnits() {

        List<Flow> flows = this.split.getFlows();

        parallelBatchWorkUnits = new ArrayList<BatchFlowInSplitWorkUnit>();

        // Build all sub jobs from flows in split
        synchronized (subJobs) {
            for (Flow flow : flows) {
                subJobs.add(PartitionedStepBuilder.buildFlowInSplitSubJob(jobExecution.getExecutionId(), jobContext, this.split, flow));
            }
            for (JSLJob job : subJobs) {
                int count = batchKernel.getJobInstanceCount(job.getId());
                FlowInSplitBuilderConfig config = new FlowInSplitBuilderConfig(job, completedWorkQueue, rootJobExecutionId);
                if (count == 0) {
                    parallelBatchWorkUnits.add(batchKernel.buildNewFlowInSplitWorkUnit(config));
                } else if (count == 1) {
                    parallelBatchWorkUnits.add(batchKernel.buildOnRestartFlowInSplitWorkUnit(config));
                } else {
                    throw new IllegalStateException("There is an inconsistency somewhere in the internal subjob creation");
                }
            }
        }
    }

    private void executeWorkUnits() {
        // Then start or restart all subjobs in parallel
        for (BatchParallelWorkUnit work : parallelBatchWorkUnits) {
            int count = batchKernel.getJobInstanceCount(work.getJobExecutionImpl().getJobInstance().getJobName());

            assert (count <= 1);

            if (count == 1) {
                batchKernel.startGeneratedJob(work);
            } else if (count > 1) {
                batchKernel.restartGeneratedJob(work);
            } else {
                throw new IllegalStateException("There is an inconsistency somewhere in the internal subjob creation");
            }
        }
    }

    private SplitExecutionStatus waitForCompletionAndAggregateStatus() {
        final SplitExecutionStatus splitStatus = new SplitExecutionStatus();

        for (final JSLJob ignored : subJobs) {
            final BatchFlowInSplitWorkUnit batchWork;
            try {
                batchWork = completedWorkQueue.take(); //wait for each thread to finish and then look at it's status
            } catch (InterruptedException e) {
                throw new BatchContainerRuntimeException(e);
            }

            final RuntimeFlowInSplitExecution flowExecution = batchWork.getJobExecutionImpl();
            final ExecutionStatus flowStatus = flowExecution.getFlowStatus();
            aggregateTerminatingStatusFromSingleFlow(null, flowStatus, splitStatus);
        }

        // If this is still set to 'null' that means all flows completed normally without terminating the job.
        splitStatus.setExtendedBatchStatus(ExtendedBatchStatus.NORMAL_COMPLETION);
        return splitStatus;
    }


    //
    // A <fail> and an uncaught exception are peers.  They each take precedence over a <stop>, which take precedence over an <end>.
    // Among peers the last one seen gets to set the exit stauts.
    //
    private ExtendedBatchStatus aggregateTerminatingStatusFromSingleFlow(final ExtendedBatchStatus aggregateStatus, final ExecutionStatus flowStatus, final SplitExecutionStatus splitStatus) {
        final String exitStatus = flowStatus.getExitStatus();
        final String restartOn = flowStatus.getRestartOn();
        final ExtendedBatchStatus flowBatchStatus = flowStatus.getExtendedBatchStatus();

        if (flowBatchStatus.equals(ExtendedBatchStatus.JSL_END) || flowBatchStatus.equals(ExtendedBatchStatus.JSL_STOP) ||
            flowBatchStatus.equals(ExtendedBatchStatus.JSL_FAIL) || flowBatchStatus.equals(ExtendedBatchStatus.EXCEPTION_THROWN)) {
            if (aggregateStatus == null) {
                setInJobContext(flowBatchStatus, exitStatus, restartOn);
                return flowBatchStatus;
            } else {
                splitStatus.setCouldMoreThanOneFlowHaveTerminatedJob(true);
                if (aggregateStatus.equals(ExtendedBatchStatus.JSL_END)) {
                    logger.warning("Current flow's batch and exit status will take precedence over and override earlier one from <end> transition element. " +
                        "Overriding, setting exit status if non-null and preparing to end job.");
                    setInJobContext(flowBatchStatus, exitStatus, restartOn);
                    return flowBatchStatus;
                } else if (aggregateStatus.equals(ExtendedBatchStatus.JSL_STOP)) {
                    // Everything but an <end> overrides a <stop>
                    if (!(flowBatchStatus.equals(ExtendedBatchStatus.JSL_END))) {
                        logger.warning("Current flow's batch and exit status will take precedence over and override earlier one from <stop> transition element. " +
                            "Overriding, setting exit status if non-null and preparing to end job.");
                        setInJobContext(flowBatchStatus, exitStatus, restartOn);
                        return flowBatchStatus;
                    }
                } else if (aggregateStatus.equals(ExtendedBatchStatus.JSL_FAIL) || aggregateStatus.equals(ExtendedBatchStatus.EXCEPTION_THROWN)) {
                    if (flowBatchStatus.equals(ExtendedBatchStatus.JSL_FAIL) || flowBatchStatus.equals(ExtendedBatchStatus.EXCEPTION_THROWN)) {
                        logger.warning("Current flow's batch and exit status will take precedence over and override earlier one from <fail> transition element or exception thrown. " +
                            "Overriding, setting exit status if non-null and preparing to end job.");
                        setInJobContext(flowBatchStatus, exitStatus, restartOn);
                        return flowBatchStatus;
                    }
                }
            }
        }

        return null;
    }

    private void setInJobContext(ExtendedBatchStatus flowBatchStatus, String exitStatus, String restartOn) {
        if (exitStatus != null) {
            jobContext.setExitStatus(exitStatus);
        }
        if (ExtendedBatchStatus.JSL_STOP.equals(flowBatchStatus)) {
            if (restartOn != null) {
                jobContext.setRestartOn(restartOn);
            }
        }
    }

    @Override
    public List<Long> getLastRunStepExecutions() {
        final List<Long> stepExecIdList = new ArrayList<Long>();
        for (final BatchFlowInSplitWorkUnit workUnit : parallelBatchWorkUnits) {
            final List<Long> stepExecIds = workUnit.getController().getLastRunStepExecutions();
            stepExecIdList.addAll(stepExecIds);
        }
        return stepExecIdList;
    }
}
