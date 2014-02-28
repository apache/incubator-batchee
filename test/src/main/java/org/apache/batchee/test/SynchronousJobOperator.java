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
package org.apache.batchee.test;

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
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * An implementation of JobOperator delegating to real JBatch implementation
 * but waiting for start/stop/restart method.
 *
 * Note: would be great to keep this class portable, if not we should just extend BatchKernel and use
 * org.apache.batchee.container.services.kernel.DefaultBatchKernel#jobExecutionDone(org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution)
 */
public class SynchronousJobOperator implements JobOperator {
    private static final Collection<BatchStatus> BATCH_END_STATUSES = Arrays.asList(BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.STOPPED, BatchStatus.ABANDONED);

    private volatile JobOperator delegate;

    private JobOperator getOrCreateDelegate() {
        if (delegate == null) {
            synchronized (this) {
                if (delegate == null) {
                    delegate = BatchRuntime.getJobOperator();
                }
            }
        }
        return delegate;
    }

    private void waitEnd(final long id) { // copy of Batches class but avoids to be linked to BatchEE
        try {
            final Class<?> batcheeOpClass = Thread.currentThread().getContextClassLoader().loadClass("org.apache.batchee.container.impl.JobOperatorImpl");
            if (batcheeOpClass.isInstance(delegate)) {
                batcheeOpClass.getMethod("waitFor", long.class).invoke(delegate, id);
                return;
            }
            return;
        } catch (final Exception e) {
            // no-op
        }

        do {
            try {
                Thread.sleep(20);
            } catch (final InterruptedException e) {
                return;
            }
        } while (!BATCH_END_STATUSES.contains(delegate.getJobExecution(id).getBatchStatus()));
    }

    @Override
    public long start(final String name, final Properties properties) throws JobStartException, JobSecurityException {
        final long id = getOrCreateDelegate().start(name, properties);
        waitEnd(id);
        return id;
    }

    @Override
    public long restart(final long id, final Properties properties)
            throws JobExecutionAlreadyCompleteException, NoSuchJobExecutionException, JobExecutionNotMostRecentException, JobRestartException, JobSecurityException {
        final long newId = getOrCreateDelegate().restart(id, properties);
        waitEnd(newId);
        return newId;
    }

    @Override
    public void stop(final long id) throws NoSuchJobExecutionException, JobExecutionNotRunningException, JobSecurityException {
        getOrCreateDelegate().stop(id);
        waitEnd(id);
    }

    @Override
    public void abandon(final long id) throws NoSuchJobExecutionException, JobExecutionIsRunningException, JobSecurityException {
        getOrCreateDelegate().abandon(id);
        waitEnd(id);
    }

    @Override
    public Set<String> getJobNames() throws JobSecurityException {
        return getOrCreateDelegate().getJobNames();
    }

    @Override
    public int getJobInstanceCount(final String name) throws NoSuchJobException, JobSecurityException {
        return getOrCreateDelegate().getJobInstanceCount(name);
    }

    @Override
    public List<JobInstance> getJobInstances(final String name, final int start, final int count) throws NoSuchJobException, JobSecurityException {
        return getOrCreateDelegate().getJobInstances(name, start, count);
    }

    @Override
    public List<Long> getRunningExecutions(final String name) throws NoSuchJobException, JobSecurityException {
        return getOrCreateDelegate().getRunningExecutions(name);
    }

    @Override
    public Properties getParameters(final long id) throws NoSuchJobExecutionException, JobSecurityException {
        return getOrCreateDelegate().getParameters(id);
    }

    @Override
    public JobInstance getJobInstance(final long id) throws NoSuchJobExecutionException, JobSecurityException {
        return getOrCreateDelegate().getJobInstance(id);
    }

    @Override
    public List<JobExecution> getJobExecutions(final JobInstance jobInstance) throws NoSuchJobInstanceException, JobSecurityException {
        return getOrCreateDelegate().getJobExecutions(jobInstance);
    }

    @Override
    public JobExecution getJobExecution(final long id) throws NoSuchJobExecutionException, JobSecurityException {
        return getOrCreateDelegate().getJobExecution(id);
    }

    @Override
    public List<StepExecution> getStepExecutions(final long id) throws NoSuchJobExecutionException, JobSecurityException {
        return getOrCreateDelegate().getStepExecutions(id);
    }
}
