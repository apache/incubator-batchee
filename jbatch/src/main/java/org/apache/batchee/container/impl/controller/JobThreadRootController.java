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

import org.apache.batchee.container.Controller;
import org.apache.batchee.container.ThreadRootController;
import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.navigator.ModelNavigator;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.JobListenerProxy;
import org.apache.batchee.container.proxy.ListenerFactory;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.status.ExecutionStatus;
import org.apache.batchee.container.status.ExtendedBatchStatus;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.jaxb.JSLJob;

import javax.batch.runtime.BatchStatus;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class JobThreadRootController implements ThreadRootController {
    private static final Logger LOGGER = Logger.getLogger(JobThreadRootController.class.getName());

    protected final RuntimeJobExecution jobExecution;
    protected final JobContextImpl jobContext;
    protected final long rootJobExecutionId;
    protected final long jobInstanceId;
    private final ListenerFactory listenerFactory;
    protected final ModelNavigator<JSLJob> jobNavigator;
    protected final JobStatusManagerService jobStatusService;
    protected final PersistenceManagerService persistenceService;

    private ExecutionTransitioner transitioner;
    private BlockingQueue<PartitionDataWrapper> analyzerQueue;

    public JobThreadRootController(final RuntimeJobExecution jobExecution, final long rootJobExecutionId) {
        this.jobExecution = jobExecution;
        this.jobContext = jobExecution.getJobContext();
        this.rootJobExecutionId = rootJobExecutionId;
        this.jobInstanceId = jobExecution.getInstanceId();
        this.jobStatusService = ServicesManager.service(JobStatusManagerService.class);
        this.persistenceService = ServicesManager.service(PersistenceManagerService.class);
        this.jobNavigator = jobExecution.getJobNavigator();

        final JSLJob jobModel = jobExecution.getJobNavigator().getRootModelElement();
        final InjectionReferences injectionRef = new InjectionReferences(jobContext, null, null);
        listenerFactory = new ListenerFactory(jobModel, injectionRef, jobExecution);
        jobExecution.setListenerFactory(listenerFactory);
    }

    /*
     * By not passing the rootJobExecutionId, we are "orphaning" the subjob execution and making it not findable from the parent.
     * This is exactly what we want for getStepExecutions()... we don't want it to get extraneous entries for the partitions.
     */
    public JobThreadRootController(final RuntimeJobExecution jobExecution, final BlockingQueue<PartitionDataWrapper> analyzerQueue) {
        this(jobExecution, jobExecution.getExecutionId());
        this.analyzerQueue = analyzerQueue;
    }

    @Override
    public ExecutionStatus originateExecutionOnThread() {
        ExecutionStatus retVal = null;
        try {
            // Check if we've already gotten the stop() command.
            if (!jobContext.getBatchStatus().equals(BatchStatus.STOPPING)) {

                // Now that we're ready to start invoking artifacts, set the status to 'STARTED'
                markJobStarted();

                jobListenersBeforeJob();

                // --------------------
                // The BIG loop transitioning
                // within the job !!!
                // --------------------
                transitioner = new ExecutionTransitioner(jobExecution, rootJobExecutionId, jobNavigator, analyzerQueue);
                retVal = transitioner.doExecutionLoop();
                ExtendedBatchStatus extBatchStatus = retVal.getExtendedBatchStatus();
                switch (extBatchStatus) {
                    case JSL_STOP:
                        jslStop();
                        break;
                    case JSL_FAIL:
                        updateJobBatchStatus(BatchStatus.FAILED);
                        break;
                    case EXCEPTION_THROWN:
                        updateJobBatchStatus(BatchStatus.FAILED);
                        break;
                }
            }
        } catch (final Throwable t) {
            // We still want to try to call the afterJob() listener and persist the batch and exit
            // status for the failure in an orderly fashion.  So catch and continue.
            batchStatusFailedFromException();
            LOGGER.log(Level.SEVERE, t.getMessage(), t);
        }

        endOfJob();

        return retVal;
    }

    protected void jslStop() {
        String restartOn = jobContext.getRestartOn();
        batchStatusStopping();
        jobStatusService.updateJobStatusFromJSLStop(jobInstanceId, restartOn);
    }

    protected void markJobStarted() {
        updateJobBatchStatus(BatchStatus.STARTED);
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        jobExecution.setLastUpdateTime(timestamp);
        jobExecution.setStartTime(timestamp);
        persistenceService.markJobStarted(jobExecution.getExecutionId(), timestamp);
    }

    /*
     *  Follow similar pattern for end of step in BaseStepController
     *
     *  1. Execute the very last artifacts (jobListener)
     *  2. transition to final batch status
     *  3. default ExitStatus if necessary
     *  4. persist statuses and end time data
     *
     *  We don't want to give up on the orderly process of 2,3,4, if we blow up
     *  in after job, so catch that and keep on going.
     */
    protected void endOfJob() {


        // 1. Execute the very last artifacts (jobListener)
        try {
            jobListenersAfterJob();
        } catch (Throwable t) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            batchStatusFailedFromException();
        }

        // 2. transition to final batch status
        transitionToFinalBatchStatus();

        // 3. default ExitStatus if necessary
        if (jobContext.getExitStatus() == null) {
            jobContext.setExitStatus(jobContext.getBatchStatus().name());
        }

        // 4. persist statuses and end time data
        persistJobBatchAndExitStatus();

    }

    private void persistJobBatchAndExitStatus() {
        BatchStatus batchStatus = jobContext.getBatchStatus();

        // Take a current timestamp for last updated no matter what the status.
        long time = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(time);
        jobExecution.setLastUpdateTime(timestamp);

        // Perhaps these should be coordinated in a tran but probably better still would be
        // rethinking the table design to let the database provide us consistently with a single update.
        jobStatusService.updateJobBatchStatus(jobInstanceId, batchStatus);
        jobStatusService.updateJobExecutionStatus(jobExecution.getInstanceId(), jobContext.getBatchStatus(), jobContext.getExitStatus());

        if (batchStatus.equals(BatchStatus.COMPLETED) || batchStatus.equals(BatchStatus.STOPPED) ||
            batchStatus.equals(BatchStatus.FAILED)) {

            jobExecution.setEndTime(timestamp);
            persistenceService.updateWithFinalExecutionStatusesAndTimestamps(jobExecution.getExecutionId(),
                batchStatus, jobContext.getExitStatus(), timestamp);
        } else {
            throw new IllegalStateException("Not expected to encounter batchStatus of " + batchStatus + " at this point.  Aborting.");
        }
    }

    /**
     * The only valid states at this point are STARTED or STOPPING.   Shouldn't have
     * been able to get to COMPLETED, STOPPED, or FAILED at this point in the code.
     */

    private void transitionToFinalBatchStatus() {
        BatchStatus currentBatchStatus = jobContext.getBatchStatus();
        if (currentBatchStatus.equals(BatchStatus.STARTED)) {
            updateJobBatchStatus(BatchStatus.COMPLETED);
        } else if (currentBatchStatus.equals(BatchStatus.STOPPING)) {
            updateJobBatchStatus(BatchStatus.STOPPED);
        } else if (currentBatchStatus.equals(BatchStatus.FAILED)) {
            updateJobBatchStatus(BatchStatus.FAILED);  // Should have already been done but maybe better for possible code refactoring to have it here.
        } else {
            throw new IllegalStateException("Step batch status should not be in a " + currentBatchStatus.name() + " state");
        }
    }

    protected void updateJobBatchStatus(BatchStatus batchStatus) {
        jobContext.setBatchStatus(batchStatus);
    }

    /*
     * The thought here is that while we don't persist all the transitions in batch status (given
     * we plan to persist at the very end), we do persist STOPPING right away, since if we end up
     * "stuck in STOPPING" we at least will have a record in the database.
     */
    protected void batchStatusStopping() {
        updateJobBatchStatus(BatchStatus.STOPPING);

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        jobExecution.setLastUpdateTime(timestamp);
        persistenceService.updateBatchStatusOnly(jobExecution.getExecutionId(), BatchStatus.STOPPING, timestamp);
    }

    @Override
    public void stop() {
        if (jobContext.getBatchStatus().equals(BatchStatus.STARTING) || jobContext.getBatchStatus().equals(BatchStatus.STARTED)) {
            batchStatusStopping();

            if (transitioner != null) {
                final Controller stoppableElementController = transitioner.getCurrentStoppableElementController();
                if (stoppableElementController != null) {
                    stoppableElementController.stop();
                }
            } // else reusage of thread, this controller was not initialized
        }
    }

    // Call beforeJob() on all the job listeners
    protected void jobListenersBeforeJob() {
        final List<JobListenerProxy> jobListeners = listenerFactory.getJobListeners();
        for (final JobListenerProxy listenerProxy : jobListeners) {
            listenerProxy.beforeJob();
        }
    }

    // Call afterJob() on all the job listeners
    private void jobListenersAfterJob() {
        final List<JobListenerProxy> jobListeners = listenerFactory.getJobListeners();
        for (final JobListenerProxy listenerProxy : jobListeners) {
            listenerProxy.afterJob();
        }
    }

    protected void batchStatusFailedFromException() {
        updateJobBatchStatus(BatchStatus.FAILED);
    }

    @Override
    public List<Long> getLastRunStepExecutions() {
        return this.transitioner.getStepExecIds();
    }
}
