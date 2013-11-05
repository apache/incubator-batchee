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

import org.apache.batchee.container.Init;
import org.apache.batchee.container.services.BatchKernelService;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.jmx.BatchEE;
import org.apache.batchee.jmx.BatchEEMBean;
import org.apache.batchee.jmx.BatchEEMBeanImpl;
import org.apache.batchee.spi.JobXMLLoaderService;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.spi.SecurityService;

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
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.batchee.container.util.ClassLoaderAwareHandler.makeLoaderAware;


public class JobOperatorImpl implements JobOperator {
    static {
        Init.doInit();

        if (Boolean.parseBoolean(ServicesManager.value("org.apache.batchee.jmx", "true"))) {
            try {
                final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                final String app = ServicesManager.value("org.apache.batchee.jmx.application", "");
                final ObjectName name;
                if (app.isEmpty()) {
                    name = new ObjectName(BatchEEMBean.DEFAULT_OBJECT_NAME);
                } else {
                    name = new ObjectName(BatchEEMBean.DEFAULT_OBJECT_NAME + ",application=" + app);
                }

                if (platformMBeanServer.isRegistered(name)) {
                    platformMBeanServer.unregisterMBean(name);
                }

                platformMBeanServer.registerMBean(
                    new BatchEE(
                        makeLoaderAware(BatchEEMBean.class, new Class<?>[]{ BatchEEMBean.class }, BatchEEMBeanImpl.INSTANCE)),
                    name);
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    enum Permissions {
        START("start");

        private final String name;

        Permissions(final String permName) {
            name = permName;
        }
    }

    public static final String JBATCH_ADMIN = "admin";

    private static final BatchKernelService KERNEL_SERVICE = ServicesManager.service(BatchKernelService.class);
    private static final PersistenceManagerService PERSISTENCE_SERVICE = ServicesManager.service(PersistenceManagerService.class);
    private static final JobXMLLoaderService XML_LOADER_SERVICE = ServicesManager.service(JobXMLLoaderService.class);
    private static final JobStatusManagerService STATUS_MANAGER_SERVICE = ServicesManager.service(JobStatusManagerService.class);
    private static final SecurityService SECURITY_SERVICE= ServicesManager.service(SecurityService.class);

    @Override
    public long start(final String jobXMLName, final Properties jobParameters) throws JobStartException, JobSecurityException {
        if (!SECURITY_SERVICE.isAuthorized(Permissions.START.name)) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }

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

        final String jobXML = XML_LOADER_SERVICE.loadJSL(jobXMLName);
        final InternalJobExecution execution = KERNEL_SERVICE.startJob(jobXML, jobParameters);
        return execution.getExecutionId();
    }

    @Override
    public void abandon(final long executionId) throws NoSuchJobExecutionException, JobExecutionIsRunningException, JobSecurityException {
        if (!SECURITY_SERVICE.isAuthorized(PERSISTENCE_SERVICE.getJobInstanceIdByExecutionId(executionId))) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }

        final InternalJobExecution jobEx = PERSISTENCE_SERVICE.jobOperatorGetJobExecution(executionId);

        // if it is not in STARTED or STARTING state, mark it as ABANDONED
        if (jobEx.getBatchStatus().equals(BatchStatus.STARTED) || jobEx.getBatchStatus().equals(BatchStatus.STARTING)) {
            throw new JobExecutionIsRunningException("Job Execution: " + executionId + " is still running");
        }

        // update table to reflect ABANDONED state
        PERSISTENCE_SERVICE.updateBatchStatusOnly(jobEx.getExecutionId(), BatchStatus.ABANDONED, new Timestamp(System.currentTimeMillis()));

        // Don't forget to update JOBSTATUS table
        STATUS_MANAGER_SERVICE.updateJobBatchStatus(jobEx.getInstanceId(), BatchStatus.ABANDONED);
    }

    @Override
    public InternalJobExecution getJobExecution(final long executionId)
        throws NoSuchJobExecutionException, JobSecurityException {
        if (!SECURITY_SERVICE.isAuthorized(PERSISTENCE_SERVICE.getJobInstanceIdByExecutionId(executionId))) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }
        return KERNEL_SERVICE.getJobExecution(executionId);
    }

    @Override
    public List<JobExecution> getJobExecutions(final JobInstance instance)
        throws NoSuchJobInstanceException, JobSecurityException {
        if (!SECURITY_SERVICE.isAuthorized(instance.getInstanceId())) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }

        // Mediate between one
        final List<JobExecution> executions = new ArrayList<JobExecution>();
        List<InternalJobExecution> executionImpls = PERSISTENCE_SERVICE.jobOperatorGetJobExecutions(instance.getInstanceId());
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
        if (!SECURITY_SERVICE.isAuthorized(PERSISTENCE_SERVICE.getJobInstanceIdByExecutionId(executionId))) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }
        return KERNEL_SERVICE.getJobInstance(executionId);
    }

    @Override
    public int getJobInstanceCount(String jobName) throws NoSuchJobException, JobSecurityException {
        int jobInstanceCount;
        if (SECURITY_SERVICE.isAuthorized(JBATCH_ADMIN)) {
            // Do an unfiltered query
            jobInstanceCount = PERSISTENCE_SERVICE.jobOperatorGetJobInstanceCount(jobName);
        } else {
            jobInstanceCount = PERSISTENCE_SERVICE.jobOperatorGetJobInstanceCount(jobName, SECURITY_SERVICE.getLoggedUser());
        }

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

        final List<Long> instanceIds;
        if (SECURITY_SERVICE.isAuthorized(JBATCH_ADMIN)) {
            // Do an unfiltered query
            instanceIds = PERSISTENCE_SERVICE.jobOperatorGetJobInstanceIds(jobName, start, count);
        } else {
            instanceIds = PERSISTENCE_SERVICE.jobOperatorGetJobInstanceIds(jobName, SECURITY_SERVICE.getLoggedUser(), start, count);
        }

        // get the jobinstance ids associated with this job name

        if (instanceIds.size() > 0) {
            // for every job instance id
            for (long id : instanceIds) {
                // get the job instance obj, add it to the list
                final JobStatus jobStatus = STATUS_MANAGER_SERVICE.getJobStatus(id);
                final JobInstance jobInstance = jobStatus.getJobInstance();
                if (SECURITY_SERVICE.isAuthorized(jobInstance.getInstanceId())) {
                    jobInstances.add(jobInstance);
                }
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
        final Set<String> jobNames = new HashSet<String>();
        final Map<Long, String> data = PERSISTENCE_SERVICE.jobOperatorGetExternalJobInstanceData();
        for (final Map.Entry<Long, String> entry : data.entrySet()) {
            long instanceId = entry.getKey();
            if (SECURITY_SERVICE.isAuthorized(instanceId)) {
                jobNames.add(entry.getValue());
            }
        }
        return jobNames;
    }

    @Override
    public Properties getParameters(final long executionId) throws NoSuchJobExecutionException, JobSecurityException {
        final JobInstance requestedJobInstance = KERNEL_SERVICE.getJobInstance(executionId);
        if (!SECURITY_SERVICE.isAuthorized(requestedJobInstance.getInstanceId())) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }
        return PERSISTENCE_SERVICE.getParameters(executionId);
    }


    @Override
    public List<Long> getRunningExecutions(final String jobName) throws NoSuchJobException, JobSecurityException {
        final List<Long> jobExecutions = new ArrayList<Long>();

        // get the jobexecution ids associated with this job name
        final Set<Long> executionIds = PERSISTENCE_SERVICE.jobOperatorGetRunningExecutions(jobName);

        if (executionIds.size() <= 0) {
            throw new NoSuchJobException("Job Name " + jobName + " not found");
        }

        // for every job instance id
        for (final long id : executionIds) {
            try {
                if (SECURITY_SERVICE.isAuthorized(PERSISTENCE_SERVICE.getJobInstanceIdByExecutionId(id))) {
                    if (KERNEL_SERVICE.isExecutionRunning(id)) {
                        final InternalJobExecution jobEx = KERNEL_SERVICE.getJobExecution(id);
                        jobExecutions.add(jobEx.getExecutionId());
                    }
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

        final InternalJobExecution jobEx = KERNEL_SERVICE.getJobExecution(executionId);
        if (jobEx == null) {
            throw new NoSuchJobExecutionException("Job Execution: " + executionId + " not found");
        }
        if (SECURITY_SERVICE.isAuthorized(PERSISTENCE_SERVICE.getJobInstanceIdByExecutionId(executionId))) {
            return PERSISTENCE_SERVICE.getStepExecutionsForJobExecution(executionId);
        }
        throw new JobSecurityException("The current user is not authorized to perform this operation");
    }

    @Override
    public long restart(long oldExecutionId, Properties restartParameters) throws JobExecutionAlreadyCompleteException, NoSuchJobExecutionException, JobExecutionNotMostRecentException, JobRestartException, JobSecurityException {
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

    private long restartInternal(final long oldExecutionId, final Properties restartParameters) throws JobExecutionAlreadyCompleteException, NoSuchJobExecutionException, JobExecutionNotMostRecentException, JobRestartException, JobSecurityException {
        if (!SECURITY_SERVICE.isAuthorized(PERSISTENCE_SERVICE.getJobInstanceIdByExecutionId(oldExecutionId))) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }

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

        return KERNEL_SERVICE.restartJob(oldExecutionId, restartParameters).getExecutionId();
    }

    @Override
    public void stop(final long executionId) throws NoSuchJobExecutionException, JobExecutionNotRunningException, JobSecurityException {
        if (!SECURITY_SERVICE.isAuthorized(PERSISTENCE_SERVICE.getJobInstanceIdByExecutionId(executionId))) {
            throw new JobSecurityException("The current user is not authorized to perform this operation");
        }

        KERNEL_SERVICE.stopJob(executionId);
    }
}