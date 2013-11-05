/**
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
package org.apache.batchee.container.util;

import org.apache.batchee.container.ThreadRootController;
import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.impl.controller.JobController;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.BatchKernelService;

import javax.batch.runtime.BatchStatus;
import java.io.PrintWriter;
import java.io.StringWriter;

/*
 * I took out the 'work type' constant since I don't see that we want to use
 * the same thread pool for start requests as we'd use for stop requests.
 * The stop seems like it should be synchronous from the JobOperator's
 * perspective, as it returns a 'success' boolean.
 */
public class BatchWorkUnit implements Runnable {
    protected RuntimeJobExecution jobExecutionImpl = null;
    protected BatchKernelService batchKernel = null;
    protected ThreadRootController controller;

    protected boolean notifyCallbackWhenDone;

    public BatchWorkUnit(final BatchKernelService batchKernel, final RuntimeJobExecution jobExecutionImpl) {
        this(batchKernel, jobExecutionImpl, true);
    }

    public BatchWorkUnit(final BatchKernelService batchKernel, final RuntimeJobExecution jobExecutionImpl,
                         final boolean notifyCallbackWhenDone) {
        this.setBatchKernel(batchKernel);
        this.setJobExecutionImpl(jobExecutionImpl);
        this.setNotifyCallbackWhenDone(notifyCallbackWhenDone);
        this.controller = new JobController(jobExecutionImpl);
    }

    public ThreadRootController getController() {
        return this.controller;
    }

    @Override
    public void run() {
        try {
            controller.originateExecutionOnThread();

            if (isNotifyCallbackWhenDone()) {
                getBatchKernel().jobExecutionDone(getJobExecutionImpl());
            }
        } catch (final Throwable t) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);

            if (isNotifyCallbackWhenDone()) {
                getBatchKernel().jobExecutionDone(getJobExecutionImpl());
            }

            throw new BatchContainerRuntimeException("This job failed unexpectedly.", t);
        } finally {
            // Put this in finally to minimize chance of tying up threads.
            markThreadCompleted();
        }
    }

    protected BatchStatus getBatchStatus() {
        return jobExecutionImpl.getJobContext().getBatchStatus();
    }

    protected String getExitStatus() {
        return jobExecutionImpl.getJobContext().getExitStatus();
    }

    public void setBatchKernel(BatchKernelService batchKernel) {
        this.batchKernel = batchKernel;
    }

    public BatchKernelService getBatchKernel() {
        return batchKernel;
    }

    public void setJobExecutionImpl(RuntimeJobExecution jobExecutionImpl) {
        this.jobExecutionImpl = jobExecutionImpl;
    }

    public RuntimeJobExecution getJobExecutionImpl() {
        return jobExecutionImpl;
    }

    public void setNotifyCallbackWhenDone(boolean notifyCallbackWhenDone) {
        this.notifyCallbackWhenDone = notifyCallbackWhenDone;
    }

    public boolean isNotifyCallbackWhenDone() {
        return notifyCallbackWhenDone;
    }

    protected void markThreadCompleted() {
        // No-op
    }
}
