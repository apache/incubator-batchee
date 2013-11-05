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
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.impl.MetricImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.StepExecutionImpl;
import org.apache.batchee.container.impl.controller.chunk.PersistentDataWrapper;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.BatchKernelService;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.status.ExecutionStatus;
import org.apache.batchee.container.status.ExtendedBatchStatus;
import org.apache.batchee.container.status.StepStatus;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.jaxb.JSLProperties;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.spi.TransactionManagementService;
import org.apache.batchee.spi.TransactionManagerAdapter;

import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionNotMostRecentException;
import javax.batch.operations.JobRestartException;
import javax.batch.operations.JobStartException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Change the name of this class to something else!! Or change BaseStepController.
 */
public abstract class BaseStepController implements ExecutionElementController {
    private static final Logger LOGGER = Logger.getLogger(BaseStepController.class.getName());

    protected RuntimeJobExecution jobExecutionImpl;
    protected JobInstance jobInstance;

    protected StepContextImpl stepContext;
    protected Step step;
    protected StepStatus stepStatus;

    protected BlockingQueue<PartitionDataWrapper> analyzerStatusQueue = null;

    protected long rootJobExecutionId;

    protected static final BatchKernelService BATCH_KERNEL = ServicesManager.service(BatchKernelService.class);
    private static final PersistenceManagerService PERSISTENCE_MANAGER_SERVICE = ServicesManager.service(PersistenceManagerService.class);
    private static final JobStatusManagerService JOB_STATUS_MANAGER_SERVICE = ServicesManager.service(JobStatusManagerService.class);

    protected TransactionManagerAdapter transactionManager = null;

    protected BaseStepController(final RuntimeJobExecution jobExecution, final Step step, final StepContextImpl stepContext, final long rootJobExecutionId) {
        this.jobExecutionImpl = jobExecution;
        this.jobInstance = jobExecution.getJobInstance();
        this.stepContext = stepContext;
        this.rootJobExecutionId = rootJobExecutionId;
        if (step == null) {
            throw new IllegalArgumentException("Step parameter to ctor cannot be null.");
        }
        this.step = step;
    }

    protected BaseStepController(final RuntimeJobExecution jobExecution,
                                 final Step step, final StepContextImpl stepContext,
                                 final long rootJobExecutionId,
                                 final BlockingQueue<PartitionDataWrapper> analyzerStatusQueue) {
        this(jobExecution, step, stepContext, rootJobExecutionId);
        this.analyzerStatusQueue = analyzerStatusQueue;
    }

    ///////////////////////////
    // ABSTRACT METHODS ARE HERE
    ///////////////////////////
    protected abstract void invokeCoreStep() throws JobRestartException, JobStartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException;

    protected abstract void setupStepArtifacts();

    protected abstract void invokePreStepArtifacts();

    protected abstract void invokePostStepArtifacts();

    // This is only useful from the partition threads
    protected abstract void sendStatusFromPartitionToAnalyzerIfPresent();

    @Override
    public ExecutionStatus execute() {
        // Here we're just setting up to decide if we're going to run the step or not (if it's already complete and
        // allow-start-if-complete=false.
        try {
            boolean executeStep = shouldStepBeExecuted();
            if (!executeStep) {
                return new ExecutionStatus(ExtendedBatchStatus.DO_NOT_RUN, stepStatus.getExitStatus());
            }
        } catch (final Throwable t) {
            // Treat an error at this point as unrecoverable, so fail job too.
            markJobAndStepFailed();
            LOGGER.log(Level.SEVERE, t.getMessage(), t);
            rethrowWithMsg("Caught throwable while determining if step should be executed.  Failing job.", t);
        }

        // At this point we have a StepExecution.  Setup so that we're ready to invoke artifacts.
        try {
            startStep();
        } catch (final Throwable t) {
            // Treat an error at this point as unrecoverable, so fail job too.
            markJobAndStepFailed();
            LOGGER.log(Level.SEVERE, t.getMessage(), t);
            rethrowWithMsg("Caught throwable while starting step.  Failing job.", t);
        }

        // At this point artifacts are in the picture so we want to try to invoke afterStep() on a failure.
        try {
            invokePreStepArtifacts();    //Call PartitionReducer and StepListener(s)
            invokeCoreStep();
        } catch (final Exception e) {
            // We're going to continue on so that we can execute the afterStep() and analyzer
            try {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
                markStepFailed();
            } catch (final Throwable t) {
                // Since the first one is the original first failure, let's rethrow t1 and not the second error,
                // but we'll log a severe error pointing out that the failure didn't get persisted..
                // We won't try to call the afterStep() in this case either.
                rethrowWithMsg("ERROR. PERSISTING BATCH STATUS FAILED.  STEP EXECUTION STATUS TABLES MIGHT HAVE CONSISTENCY ISSUES" +
                    "AND/OR UNEXPECTED ENTRIES.", t);
            }
        } catch (final Throwable t) {
            LOGGER.log(Level.SEVERE, t.getMessage(), t);
            markJobAndStepFailed();
        }

        //
        // At this point we may have already failed the step, but we still try to invoke the end of step artifacts.
        //
        try {
            //Call PartitionAnalyzer, PartitionReducer and StepListener(s)
            invokePostStepArtifacts();
        } catch (final Throwable t) {
            LOGGER.log(Level.SEVERE, t.getMessage(), t);
            markStepFailed();
        }

        //
        // No more application code is on the path from here on out (excluding the call to the PartitionAnalyzer
        // analyzeStatus().  If an exception bubbles up and leaves the statuses inconsistent or incorrect then so be it;
        // maybe there's a runtime bug that will need to be fixed.
        //
        try {
            // Now that all step-level artifacts have had a chance to run,
            // we set the exit status to one of the defaults if it is still unset.

            // This is going to be the very last sequence of calls from the step running on the main thread,
            // since the call back to the partition analyzer only happens on the partition threads.
            // On the partition threads, then, we harden the status at the partition level before we
            // send it back to the main thread.
            persistUserData();
            transitionToFinalBatchStatus();
            defaultExitStatusIfNecessary();
            persistExitStatusAndEndTimestamp();
        } catch (final Throwable t) {
            // Don't let an exception caught here prevent us from persisting the failed batch status.
            markJobAndStepFailed();
            rethrowWithMsg("Failure ending step execution", t);
        }

        //
        // Only happens on main thread.
        //
        sendStatusFromPartitionToAnalyzerIfPresent();

        if (stepStatus.getBatchStatus().equals(BatchStatus.FAILED)) {
            return new ExecutionStatus(ExtendedBatchStatus.EXCEPTION_THROWN, stepStatus.getExitStatus());
        }
        return new ExecutionStatus(ExtendedBatchStatus.NORMAL_COMPLETION, stepStatus.getExitStatus());
    }

    private void defaultExitStatusIfNecessary() {
        final String stepExitStatus = stepContext.getExitStatus();
        final String processRetVal = stepContext.getBatchletProcessRetVal();
        if (stepExitStatus == null) {
            if (processRetVal != null) {
                stepContext.setExitStatus(processRetVal);
            } else {
                stepContext.setExitStatus(stepContext.getBatchStatus().name());
            }
        }
    }

    private void markStepFailed() {
        updateBatchStatus(BatchStatus.FAILED);
    }

    protected void markJobAndStepFailed() {
        jobExecutionImpl.getJobContext().setBatchStatus(BatchStatus.FAILED);
        markStepFailed();
    }

    private void startStep() {
        // Update status
        statusStarting();
        //Set Step context properties
        setContextProperties();
        //Set up step artifacts like step listeners, partition reducers
        setupStepArtifacts();
        // Move batch status to started.
        updateBatchStatus(BatchStatus.STARTED);

        long time = System.currentTimeMillis();
        Timestamp startTS = new Timestamp(time);
        stepContext.setStartTime(startTS);

        PERSISTENCE_MANAGER_SERVICE.updateStepExecution(rootJobExecutionId, stepContext);
    }


    /**
     * The only valid states at this point are STARTED,STOPPING, or FAILED.
     * been able to get to STOPPED, or COMPLETED yet at this point in the code.
     */
    private void transitionToFinalBatchStatus() {
        BatchStatus currentBatchStatus = stepContext.getBatchStatus();
        if (currentBatchStatus.equals(BatchStatus.STARTED)) {
            updateBatchStatus(BatchStatus.COMPLETED);
        } else if (currentBatchStatus.equals(BatchStatus.STOPPING)) {
            updateBatchStatus(BatchStatus.STOPPED);
        } else if (currentBatchStatus.equals(BatchStatus.FAILED)) {
            updateBatchStatus(BatchStatus.FAILED);           // Should have already been done but maybe better for possible code refactoring to have it here.
        } else {
            throw new IllegalStateException("Step batch status should not be in a " + currentBatchStatus.name() + " state");
        }
    }

    protected void updateBatchStatus(final BatchStatus updatedBatchStatus) {
        stepStatus.setBatchStatus(updatedBatchStatus);
        JOB_STATUS_MANAGER_SERVICE.updateStepStatus(stepStatus.getStepExecutionId(), stepStatus);
        stepContext.setBatchStatus(updatedBatchStatus);
    }

    protected boolean shouldStepBeExecuted() {
        this.stepStatus = JOB_STATUS_MANAGER_SERVICE.getStepStatus(jobInstance.getInstanceId(), step.getId());
        if (stepStatus == null) {
            // create new step execution
            final StepExecutionImpl stepExecution = getNewStepExecution(rootJobExecutionId, stepContext);
            // create new step status for this run
            stepStatus = JOB_STATUS_MANAGER_SERVICE.createStepStatus(stepExecution.getStepExecutionId());
            stepContext.setStepExecutionId(stepExecution.getStepExecutionId());
            return true;
        } else {
            // if a step status already exists for this instance id. It means this
            // is a restart and we need to get the previously persisted data
            stepContext.setPersistentUserData(stepStatus.getPersistentUserData());
            if (shouldStepBeExecutedOnRestart()) {
                // Seems better to let the start count get incremented without getting a step execution than
                // vice versa (in an unexpected error case).
                stepStatus.incrementStartCount();
                // create new step execution
                final StepExecutionImpl stepExecution = getNewStepExecution(rootJobExecutionId, stepContext);
                this.stepStatus.setLastRunStepExecutionId(stepExecution.getStepExecutionId());
                stepContext.setStepExecutionId(stepExecution.getStepExecutionId());
                return true;
            }
            return false;
        }
    }

    private boolean shouldStepBeExecutedOnRestart() {
        BatchStatus stepBatchStatus = stepStatus.getBatchStatus();
        if (stepBatchStatus.equals(BatchStatus.COMPLETED)) {
            // A bit of parsing involved since the model gives us a String not a
            // boolean, but it should default to 'false', which is the spec'd default.
            if (!Boolean.parseBoolean(step.getAllowStartIfComplete())) {
                return false;
            }
        }

        // The spec default is '0', which we get by initializing to '0' in the next line
        int startLimit = 0;
        String startLimitString = step.getStartLimit();
        if (startLimitString != null) {
            try {
                startLimit = Integer.parseInt(startLimitString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse start limit value.  Received NumberFormatException for start-limit value:  " + startLimitString
                    + " for stepId: " + step.getId() + ", with start-limit=" + step.getStartLimit());
            }
        }

        if (startLimit < 0) {
            throw new IllegalArgumentException("Found negative start-limit of " + startLimit + "for stepId: " + step.getId());
        }

        if (startLimit > 0) {
            final int newStepStartCount = stepStatus.getStartCount() + 1;
            if (newStepStartCount > startLimit) {
                throw new IllegalStateException("For stepId: " + step.getId() + ", tried to start step for the " + newStepStartCount
                    + " time, but startLimit = " + startLimit);
            }
        }
        return true;
    }


    protected void statusStarting() {
        stepStatus.setBatchStatus(BatchStatus.STARTING);
        JOB_STATUS_MANAGER_SERVICE.updateJobCurrentStep(jobInstance.getInstanceId(), step.getId());
        JOB_STATUS_MANAGER_SERVICE.updateStepStatus(stepStatus.getStepExecutionId(), stepStatus);
        stepContext.setBatchStatus(BatchStatus.STARTING);
    }

    protected void persistUserData() {
        final ByteArrayOutputStream persistentBAOS = new ByteArrayOutputStream();
        final ObjectOutputStream persistentDataOOS;

        try {
            persistentDataOOS = new ObjectOutputStream(persistentBAOS);
            persistentDataOOS.writeObject(stepContext.getPersistentUserData());
            persistentDataOOS.close();
        } catch (final Exception e) {
            throw new BatchContainerServiceException("Cannot persist the persistent user data for the step.", e);
        }

        stepStatus.setPersistentUserData(new PersistentDataWrapper(persistentBAOS.toByteArray()));
        JOB_STATUS_MANAGER_SERVICE.updateStepStatus(stepStatus.getStepExecutionId(), stepStatus);
    }

    protected void persistExitStatusAndEndTimestamp() {
        stepStatus.setExitStatus(stepContext.getExitStatus());
        JOB_STATUS_MANAGER_SERVICE.updateStepStatus(stepStatus.getStepExecutionId(), stepStatus);

        // set the end time metric before flushing
        long time = System.currentTimeMillis();
        Timestamp endTS = new Timestamp(time);
        stepContext.setEndTime(endTS);

        PERSISTENCE_MANAGER_SERVICE.updateStepExecution(rootJobExecutionId, stepContext);
    }

    private StepExecutionImpl getNewStepExecution(long rootJobExecutionId, StepContextImpl stepContext) {
        return PERSISTENCE_MANAGER_SERVICE.createStepExecution(rootJobExecutionId, stepContext);
    }

    private void setContextProperties() {
        JSLProperties jslProps = step.getProperties();

        if (jslProps != null) {
            for (Property property : jslProps.getPropertyList()) {
                Properties contextProps = stepContext.getProperties();
                contextProps.setProperty(property.getName(), property.getValue());
            }
        }

        // set up metrics
        stepContext.addMetric(MetricImpl.MetricType.READ_COUNT, 0);
        stepContext.addMetric(MetricImpl.MetricType.WRITE_COUNT, 0);
        stepContext.addMetric(MetricImpl.MetricType.READ_SKIP_COUNT, 0);
        stepContext.addMetric(MetricImpl.MetricType.PROCESS_SKIP_COUNT, 0);
        stepContext.addMetric(MetricImpl.MetricType.WRITE_SKIP_COUNT, 0);
        stepContext.addMetric(MetricImpl.MetricType.FILTER_COUNT, 0);
        stepContext.addMetric(MetricImpl.MetricType.COMMIT_COUNT, 0);
        stepContext.addMetric(MetricImpl.MetricType.ROLLBACK_COUNT, 0);

        transactionManager = ServicesManager.service(TransactionManagementService.class).getTransactionManager(stepContext);
    }

    public void setStepContext(final StepContextImpl stepContext) {
        this.stepContext = stepContext;
    }

    @Override
    public List<Long> getLastRunStepExecutions() {
        final List<Long> stepExecIdList = new ArrayList<Long>(1);
        stepExecIdList.add(this.stepStatus.getLastRunStepExecutionId());
        return stepExecIdList;
    }

    private void rethrowWithMsg(final String msgBeginning, final Throwable t) {
        final String errorMsg = msgBeginning + " ; Caught exception/error: " + t.getLocalizedMessage();
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        throw new BatchContainerRuntimeException(errorMsg, t);
    }

    public String toString() {
        return "BaseStepController for step = " + step.getId();
    }
}
