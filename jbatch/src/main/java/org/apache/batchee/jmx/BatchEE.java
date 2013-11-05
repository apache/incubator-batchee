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
package org.apache.batchee.jmx;

import javax.management.openmbean.TabularData;

public class BatchEE implements BatchEEMBean {
    private final BatchEEMBean delegate;

    public BatchEE(final BatchEEMBean delegate) {
        this.delegate = delegate;
    }

    @Override
    public String[] getJobNames() {
        return delegate.getJobNames();
    }

    @Override
    public int getJobInstanceCount(final String jobName) {
        return delegate.getJobInstanceCount(jobName);
    }

    @Override
    public TabularData getJobInstances(final String jobName, final int start, final int count) {
        return delegate.getJobInstances(jobName, start, count);
    }

    @Override
    public Long[] getRunningExecutions(final String jobName) {
        return delegate.getRunningExecutions(jobName);
    }

    @Override
    public TabularData getParameters(final long executionId) {
        return delegate.getParameters(executionId);
    }

    @Override
    public TabularData getJobInstance(final long executionId) {
        return delegate.getJobInstance(executionId);
    }

    @Override
    public TabularData getJobExecutions(final long id, final String name) {
        return delegate.getJobExecutions(id, name);
    }

    @Override
    public TabularData getJobExecution(final long executionId) {
        return delegate.getJobExecution(executionId);
    }

    @Override
    public TabularData getStepExecutions(final long jobExecutionId) {
        return delegate.getStepExecutions(jobExecutionId);
    }

    @Override
    public long start(final String jobXMLName, final String jobParameters) {
        return delegate.start(jobXMLName, jobParameters);
    }

    @Override
    public long restart(final long executionId, final String restartParameters) {
        return delegate.restart(executionId, restartParameters);
    }

    @Override
    public void stop(final long executionId) {
        delegate.stop(executionId);
    }

    @Override
    public void abandon(final long executionId) {
        delegate.abandon(executionId);
    }
}
