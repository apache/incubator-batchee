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
package org.apache.batchee.container.services.callback;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.BatchKernelService;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.spi.JobExecutionCallbackService;
import org.apache.batchee.util.Batches;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class SimpleJobExecutionCallbackService implements JobExecutionCallbackService {
    private final ConcurrentMap<Long, Collection<CountDownLatch>> waiters = new ConcurrentHashMap<Long, Collection<CountDownLatch>>();

    @Override
    public void onJobExecutionDone(final RuntimeJobExecution jobExecution) {
        final Collection<CountDownLatch> toRealease = waiters.remove(jobExecution.getExecutionId());
        if (toRealease != null) {
            for (final CountDownLatch latch : toRealease) {
                latch.countDown();
            }
        }
    }

    @Override
    public void waitFor(final long id) {
        Collection<CountDownLatch> toRelease = waiters.get(id);
        if (toRelease == null) {
            toRelease = new CopyOnWriteArrayList<CountDownLatch>();
            final Collection<CountDownLatch> existing = waiters.putIfAbsent(id, toRelease);
            if (existing != null) {
                toRelease = existing;
            }
        }

        // check before blocking
        final InternalJobExecution finalCheckExec = ServicesManager.find().service(BatchKernelService.class).getJobExecution(id);
        if (finalCheckExec != null && Batches.isDone(finalCheckExec.getBatchStatus())) {
            waiters.remove(id);
            return;
        }

        final CountDownLatch latch = new CountDownLatch(1);
        toRelease.add(latch);
        try {
            latch.await();
            waiters.remove(id);
        } catch (final InterruptedException e) {
            throw new BatchContainerRuntimeException(e);
        }
    }

    @Override
    public void init(final Properties batchConfig) {
        // no-op
    }
}
