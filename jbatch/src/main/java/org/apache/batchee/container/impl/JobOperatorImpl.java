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
package org.apache.batchee.container.impl;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionIsRunningException;
import javax.batch.operations.JobExecutionNotMostRecentException;
import javax.batch.operations.JobExecutionNotRunningException;
import javax.batch.operations.JobOperator;
import javax.batch.operations.JobRestartException;
import javax.batch.operations.JobSecurityException;
import javax.batch.operations.JobStartException;
import javax.batch.operations.NoSuchJobException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.operations.NoSuchJobInstanceException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;

import org.apache.batchee.container.Init;
import org.apache.batchee.container.services.BatchKernelService;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.spi.JobExecutionCallbackService;
import org.apache.batchee.spi.JobXMLLoaderService;
import org.apache.batchee.spi.PersistenceManagerService;


public class JobOperatorImpl implements JobOperator {
    private static final Logger LOGGER = Logger.getLogger(JobOperatorImpl.class.getName());

    static {
        Init.doInit();
    }

    private final BatchKernelService kernelService;
    private final PersistenceManagerService persistenceManagerService;
    private final JobXMLLoaderService xmlLoaderService;
    private final JobStatusManagerService statusManagerService;
    private final JobExecutionCallbackService callbackService;

    protected JobOperatorImpl(final ServicesManager servicesManager) {
        try {
            kernelService = servicesManager.service(BatchKernelService.class);
            persistenceManagerService = servicesManager.service(PersistenceManagerService.class);
            xmlLoaderService = servicesManager.service(JobXMLLoaderService.class);
            statusManagerService = servicesManager.service(JobStatusManagerService.class);
            callbackService = servicesManager.service(JobExecutionCallbackService.class);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Error while booting BatchEE", e);
            throw e;
        }
    }

    public JobOperatorImpl() {
        this(ServicesManager.find());
    }

    @Override
    public long start(final String jobXMLName, final Properties jobParameters) throws JobStartException, JobSecurityException {
        /*
         * The whole point of this method is to have JobStartException serve as a blanket exception for anything other
         * than the rest of the more specific exceptions declared on the throws clause.  So we won't log but just rethrow.
         */
        try {
            return startInternal(jobXMLName, jobParameters);
        } catch (final JobSecurityException e) {
            throw e;
        } catch (final Exception e) {
            throw new JobStartException(e);
        }
    }

    private long startInternal(final String jobXMLName, final Properties jobParameters) throws JobStartException, JobSecurityException {
        final StringWriter jobParameterWriter = new StringWriter();
        if (jobParameters != null) {
            try {
                jobParameters.store(jobParameterWriter, "Job parameters on start: ");
            } catch (IOException e) {
                jobParameterWriter.write("Job parameters on start: not printable");
            }
        } else {
            jobParameterWriter.write("Job parameters on start = null");
        }

        final String jobXML = xmlLoaderService.loadJSL(jobXMLName);
        final InternalJobExecution execution = kernelService.startJob(jobXML, jobParameters);
        return execution.getExecutionId();
    }

    @Override
    public void abandon(final long executionId) throws NoSuchJobExecutionException, JobExecutionIsRunningException, JobSecurityException {
        final InternalJobExecution jobEx = persistenceManagerService.jobOperatorGetJobExecution(executionId);

        // if it is not in STARTED or STARTING state, mark it as ABANDONED
        BatchStatus status = jobEx.getBatchStatus();
        if (status == BatchStatus.STARTING ||  status == BatchStatus.STARTED) {
            throw new JobExecutionIsRunningException("Job Execution: " + executionId + " is still running");
        }

        // update table to reflect ABANDONED state
        persistenceManagerService.updateBatchStatusOnly(jobEx.getExecutionId(), BatchStatus.ABANDONED, new Timestamp(System.currentTimeMillis()));

        // Don't forget to update JOBSTATUS table
        statusManagerService.updateJobBatchStatus(jobEx.getInstanceId(), BatchStatus.ABANDONED);
    }

    @Override
    public InternalJobExecution getJobExecution(final long executionId)
        throws NoSuchJobExecutionException, JobSecurityException {
        return kernelService.getJobExecution(executionId);
    }

    @Override
    public List<JobExecution> getJobExecutions(final JobInstance instance)
        throws NoSuchJobInstanceException, JobSecurityException {

        // Mediate between one
        final List<JobExecution> executions = new ArrayList<JobExecution>();
        List<InternalJobExecution> executionImpls = persistenceManagerService.jobOperatorGetJobExecutions(instance.getInstanceId());
        if (executionImpls.size() == 0) {
            throw new NoSuchJobInstanceException("Job: " + instance.getJobName() + " does not exist");
        }
        for (InternalJobExecution e : executionImpls) {
            executions.add(e);
        }
        return executions;
    }

    @Override
    public JobInstance getJobInstance(long executionId)
        throws NoSuchJobExecutionException, JobSecurityException {
        return kernelService.getJobInstance(executionId);
    }

    @Override
    public int getJobInstanceCount(String jobName) throws NoSuchJobException, JobSecurityException {
        final int jobInstanceCount = persistenceManagerService.jobOperatorGetJobInstanceCount(jobName);
        if (jobInstanceCount > 0) {
            return jobInstanceCount;
        }
        throw new NoSuchJobException("Job " + jobName + " not found");
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName, int start,
                                             int count) throws NoSuchJobException, JobSecurityException {

        List<JobInstance> jobInstances = new ArrayList<JobInstance>();

        if (count == 0) {
            return new ArrayList<JobInstance>();
        } else if (count < 0) {
            throw new IllegalArgumentException("Count should be a positive integer (or 0, which will return an empty list)");
        }

        final List<Long> instanceIds = persistenceManagerService.jobOperatorGetJobInstanceIds(jobName, start, count);

        // get the jobinstance ids associated with this job name

        if (instanceIds.size() > 0) {
            // for every job instance id
            for (long id : instanceIds) {
                // get the job instance obj, add it to the list
                final JobStatus jobStatus = statusManagerService.getJobStatus(id);
                final JobInstance jobInstance = jobStatus.getJobInstance();
                jobInstances.add(jobInstance);
            }
            // send the list of objs back to caller
            return jobInstances;
        }

        throw new NoSuchJobException("Job Name " + jobName + " not found");
    }

    /*
     * This should only be called by the "external" JobOperator API, since it filters
     * out the "subjob" parallel execution entries.
     */
    @Override
    public Set<String> getJobNames() throws JobSecurityException {
        return new HashSet<String>(persistenceManagerService.getJobNames());
    }

    @Override
    public Properties getParameters(final long executionId) throws NoSuchJobExecutionException, JobSecurityException {
        final JobInstance requestedJobInstance = kernelService.getJobInstance(executionId);
        return persistenceManagerService.getParameters(executionId);
    }


    @Override
    public List<Long> getRunningExecutions(final String jobName) throws NoSuchJobException, JobSecurityException {
        final List<Long> jobExecutions = new ArrayList<Long>();

        // get the jobexecution ids associated with this job name
        final Set<Long> executionIds = persistenceManagerService.jobOperatorGetRunningExecutions(jobName);

        if (executionIds.isEmpty()) {
            throw new NoSuchJobException("Job Name " + jobName + " not found");
        }

        // for every job instance id
        for (final long id : executionIds) {
            try {
                if (kernelService.isExecutionRunning(id)) {
                    final InternalJobExecution jobEx = kernelService.getJobExecution(id);
                    jobExecutions.add(jobEx.getExecutionId());
                }
            } catch (final NoSuchJobExecutionException e) {
                throw new IllegalStateException("Just found execution with id = " + id + " in table, but now seeing it as gone", e);
            }
        }
        return jobExecutions;
    }

    @Override
    public List<StepExecution> getStepExecutions(long executionId)
        throws NoSuchJobExecutionException, JobSecurityException {

        final InternalJobExecution jobEx = kernelService.getJobExecution(executionId);
        if (jobEx == null) {
            throw new NoSuchJobExecutionException("Job Execution: " + executionId + " not found");
        }
        return persistenceManagerService.getStepExecutionsForJobExecution(executionId);
    }

    @Override
    public long restart(long oldExecutionId, Properties restartParameters)
            throws JobExecutionAlreadyCompleteException, NoSuchJobExecutionException, JobExecutionNotMostRecentException, JobRestartException, JobSecurityException {
        /*
         * The whole point of this method is to have JobRestartException serve as a blanket exception for anything other
         * than the rest of the more specific exceptions declared on the throws clause.  So we won't log but just rethrow.
         */
        try {
            return restartInternal(oldExecutionId, restartParameters);
        } catch (JobExecutionAlreadyCompleteException e) {
            throw e;
        } catch (NoSuchJobExecutionException e) {
            throw e;
        } catch (JobExecutionNotMostRecentException e) {
            throw e;
        } catch (JobSecurityException e) {
            throw e;
        } catch (Exception e) {
            throw new JobRestartException(e);
        }
    }

    private long restartInternal(final long oldExecutionId, final Properties restartParameters)
            throws JobExecutionAlreadyCompleteException, NoSuchJobExecutionException, JobExecutionNotMostRecentException, JobRestartException, JobSecurityException {
        final StringWriter jobParameterWriter = new StringWriter();
        if (restartParameters != null) {
            try {
                restartParameters.store(jobParameterWriter, "Job parameters on restart: ");
            } catch (IOException e) {
                jobParameterWriter.write("Job parameters on restart: not printable");
            }
        } else {
            jobParameterWriter.write("Job parameters on restart = null");
        }

        return kernelService.restartJob(oldExecutionId, restartParameters).getExecutionId();
    }

    @Override
    public void stop(final long executionId) throws NoSuchJobExecutionException, JobExecutionNotRunningException, JobSecurityException {
        kernelService.stopJob(executionId);
    }

    public void waitFor(final long id) {
        callbackService.waitFor(id);
    }
}
