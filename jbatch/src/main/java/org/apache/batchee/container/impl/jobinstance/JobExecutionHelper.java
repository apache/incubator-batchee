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
package org.apache.batchee.container.impl.jobinstance;

import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.impl.JobInstanceImpl;
import org.apache.batchee.container.jsl.JobModelResolver;
import org.apache.batchee.container.modelresolver.PropertyResolver;
import org.apache.batchee.container.modelresolver.PropertyResolverFactory;
import org.apache.batchee.container.navigator.ModelNavigator;
import org.apache.batchee.container.navigator.NavigatorFactory;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.status.JobStatus;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.JSLProperties;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.spi.SecurityService;

import javax.batch.operations.JobExecutionAlreadyCompleteException;
import javax.batch.operations.JobExecutionNotMostRecentException;
import javax.batch.operations.JobRestartException;
import javax.batch.operations.JobStartException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import java.util.Properties;

public class JobExecutionHelper {

    private JobExecutionHelper() {
        // private utility class ct
    }

    private static ModelNavigator<JSLJob> getResolvedJobNavigator(final String jobXml, final Properties jobParameters, final boolean parallelExecution) {
        final JSLJob jobModel = new JobModelResolver().resolveModel(jobXml);
        final PropertyResolver<JSLJob> propResolver = PropertyResolverFactory.createJobPropertyResolver(parallelExecution);
        propResolver.substituteProperties(jobModel, jobParameters);
        return NavigatorFactory.createJobNavigator(jobModel);
    }

    private static ModelNavigator<JSLJob> getResolvedJobNavigator(final JSLJob jobModel, final Properties jobParameters, final boolean parallelExecution) {
        final PropertyResolver<JSLJob> propResolver = PropertyResolverFactory.createJobPropertyResolver(parallelExecution);
        propResolver.substituteProperties(jobModel, jobParameters);
        return NavigatorFactory.createJobNavigator(jobModel);
    }

    private static JobContextImpl getJobContext(ModelNavigator<JSLJob> jobNavigator) {
        final JSLProperties jslProperties;
        if (jobNavigator.getRootModelElement() != null) {
            jslProperties = jobNavigator.getRootModelElement().getProperties();
        } else {
            jslProperties = new JSLProperties();
        }
        return new JobContextImpl(jobNavigator, jslProperties);
    }

    private static JobInstance getNewJobInstance(final ServicesManager servicesManager, final String name, final String jobXml) {
        return servicesManager.service(PersistenceManagerService.class).createJobInstance(
                name, servicesManager.service(SecurityService.class).getLoggedUser(), jobXml);
    }

    private static JobInstance getNewSubJobInstance(final ServicesManager servicesManager, final String name) {
        return servicesManager.service(PersistenceManagerService.class).createSubJobInstance(
                name, servicesManager.service(SecurityService.class).getLoggedUser());
    }

    private static JobStatus createNewJobStatus(final JobStatusManagerService statusManagerService, final JobInstance jobInstance) {
        final long instanceId = jobInstance.getInstanceId();
        final JobStatus jobStatus = statusManagerService.createJobStatus(instanceId);
        jobStatus.setJobInstance(jobInstance);
        return jobStatus;
    }

    private static void validateRestartableFalseJobsDoNotRestart(final JSLJob jobModel)
        throws JobRestartException {
        if (jobModel.getRestartable() != null && jobModel.getRestartable().equalsIgnoreCase("false")) {
            throw new JobRestartException("Job Restartable attribute is false, Job cannot be restarted.");
        }
    }

    public static RuntimeJobExecution startJob(final ServicesManager servicesManager, final String jobXML, final Properties jobParameters) throws JobStartException {
        final JSLJob jobModel = new JobModelResolver().resolveModel(jobXML);
        final ModelNavigator<JSLJob> jobNavigator = getResolvedJobNavigator(jobModel, jobParameters, false);
        final JobContextImpl jobContext = getJobContext(jobNavigator);
        final JobInstance jobInstance = getNewJobInstance(servicesManager, jobNavigator.getRootModelElement().getId(), jobXML);
        final RuntimeJobExecution executionHelper =
                servicesManager.service(PersistenceManagerService.class).createJobExecution(jobInstance, jobParameters, jobContext.getBatchStatus());

        executionHelper.prepareForExecution(jobContext);

        final JobStatusManagerService statusManagerService = servicesManager.service(JobStatusManagerService.class);
        final JobStatus jobStatus = createNewJobStatus(statusManagerService, jobInstance);
        statusManagerService.updateJobStatus(jobStatus);

        return executionHelper;
    }

    public static RuntimeFlowInSplitExecution startFlowInSplit(final ServicesManager servicesManager, final JSLJob jobModel) throws JobStartException {
        final ModelNavigator<JSLJob> jobNavigator = getResolvedJobNavigator(jobModel, null, true);
        final JobContextImpl jobContext = getJobContext(jobNavigator);
        final JobInstance jobInstance = getNewSubJobInstance(servicesManager, jobNavigator.getRootModelElement().getId());
        final RuntimeFlowInSplitExecution executionHelper =
                servicesManager.service(PersistenceManagerService.class).createFlowInSplitExecution(jobInstance, jobContext.getBatchStatus());

        executionHelper.prepareForExecution(jobContext);

        final JobStatusManagerService statusManagerService = servicesManager.service(JobStatusManagerService.class);
        final JobStatus jobStatus = createNewJobStatus(statusManagerService, jobInstance);
        statusManagerService.updateJobStatus(jobStatus);

        return executionHelper;
    }

    public static RuntimeJobExecution startPartition(final ServicesManager servicesManager, final JSLJob jobModel, final Properties jobParameters) throws JobStartException {
        final ModelNavigator<JSLJob> jobNavigator = getResolvedJobNavigator(jobModel, jobParameters, true);
        final JobContextImpl jobContext = getJobContext(jobNavigator);

        final JobInstance jobInstance = getNewSubJobInstance(servicesManager, jobNavigator.getRootModelElement().getId());

        final RuntimeJobExecution executionHelper =
                servicesManager.service(PersistenceManagerService.class).createJobExecution(jobInstance, jobParameters, jobContext.getBatchStatus());

        executionHelper.prepareForExecution(jobContext);

        final JobStatusManagerService statusManagerService = servicesManager.service(JobStatusManagerService.class);
        final JobStatus jobStatus = createNewJobStatus(statusManagerService, jobInstance);
        statusManagerService.updateJobStatus(jobStatus);

        return executionHelper;
    }

    private static void validateJobInstanceNotCompleteOrAbandonded(final JobStatus jobStatus) throws JobRestartException, JobExecutionAlreadyCompleteException {
        if (jobStatus.getBatchStatus() == null) {
            throw new IllegalStateException("On restart, we didn't find an earlier batch status.");
        }

        if (jobStatus.getBatchStatus().equals(BatchStatus.COMPLETED)) {
            throw new JobExecutionAlreadyCompleteException("Already completed job instance = " + jobStatus.getJobInstanceId());
        } else if (jobStatus.getBatchStatus().equals(BatchStatus.ABANDONED)) {
            throw new JobRestartException("Abandoned job instance = " + jobStatus.getJobInstanceId());
        }
    }

    private static void validateJobExecutionIsMostRecent(final PersistenceManagerService persistenceManagerService, final long jobInstanceId, final long executionId)
            throws JobExecutionNotMostRecentException {
        final long mostRecentExecutionId = persistenceManagerService.getMostRecentExecutionId(jobInstanceId);
        if (mostRecentExecutionId != executionId) {
            throw new JobExecutionNotMostRecentException("ExecutionId: " + executionId + " is not the most recent execution.");
        }
    }

    public static RuntimeJobExecution restartPartition(final ServicesManager servicesManager, final long execId, final JSLJob gennedJobModel, final Properties partitionProps)
            throws JobRestartException,
        JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {
        return restartExecution(servicesManager, execId, gennedJobModel, partitionProps, true, false);
    }

    public static RuntimeFlowInSplitExecution restartFlowInSplit(final ServicesManager servicesManager, final long execId, final JSLJob gennedJobModel)
            throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {
        return (RuntimeFlowInSplitExecution) restartExecution(servicesManager, execId, gennedJobModel, null, true, true);
    }

    public static RuntimeJobExecution restartJob(final ServicesManager servicesManager, final long executionId, final Properties restartJobParameters)
            throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {
        return restartExecution(servicesManager, executionId, null, restartJobParameters, false, false);
    }

    private static RuntimeJobExecution restartExecution(final ServicesManager servicesManager, final long executionId, final JSLJob gennedJobModel,
                                                        final Properties restartJobParameters, final boolean parallelExecution, final boolean flowInSplit)
            throws JobRestartException, JobExecutionAlreadyCompleteException, JobExecutionNotMostRecentException, NoSuchJobExecutionException {

        final PersistenceManagerService persistenceManagerService = servicesManager.service(PersistenceManagerService.class);
        final JobStatusManagerService jobStatusManagerService = servicesManager.service(JobStatusManagerService.class);

        final long jobInstanceId = persistenceManagerService.getJobInstanceIdByExecutionId(executionId);
        final JobStatus jobStatus = jobStatusManagerService.getJobStatus(jobInstanceId);

        validateJobExecutionIsMostRecent(persistenceManagerService, jobInstanceId, executionId);

        validateJobInstanceNotCompleteOrAbandonded(jobStatus);

        final JobInstanceImpl jobInstance = jobStatus.getJobInstance();

        final ModelNavigator<JSLJob> jobNavigator;
        // If we are in a parallel job that is genned use the regenned JSL.
        if (gennedJobModel == null) {
            jobNavigator = getResolvedJobNavigator(jobInstance.getJobXML(), restartJobParameters, parallelExecution);
        } else {
            jobNavigator = getResolvedJobNavigator(gennedJobModel, restartJobParameters, parallelExecution);
        }
        // JSLJob jobModel = ModelResolverFactory.createJobResolver().resolveModel(jobInstance.getJobXML());
        validateRestartableFalseJobsDoNotRestart(jobNavigator.getRootModelElement());

        final JobContextImpl jobContext = getJobContext(jobNavigator);

        final RuntimeJobExecution executionHelper;
        if (flowInSplit) {
            executionHelper = persistenceManagerService.createFlowInSplitExecution(jobInstance, jobContext.getBatchStatus());
        } else {
            executionHelper = persistenceManagerService.createJobExecution(jobInstance, restartJobParameters, jobContext.getBatchStatus());
        }
        executionHelper.prepareForExecution(jobContext, jobStatus.getRestartOn());
        jobStatusManagerService.updateJobStatusWithNewExecution(jobInstance.getInstanceId(), executionHelper.getExecutionId());

        return executionHelper;
    }

    public static InternalJobExecution getPersistedJobOperatorJobExecution(final PersistenceManagerService persistenceManagerService, final long jobExecutionId)
            throws NoSuchJobExecutionException {
        return persistenceManagerService.jobOperatorGetJobExecution(jobExecutionId);
    }


    public static JobInstance getJobInstance(final JobStatusManagerService statusManagerService, final long executionId) {
        return statusManagerService.getJobStatusFromExecutionId(executionId).getJobInstance();
    }
}
