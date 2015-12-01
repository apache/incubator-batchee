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
package org.apache.batchee.extras.async;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.locator.BeanLocator;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemProcessor;
import javax.inject.Inject;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Documentation("Processes asynchronously the items and send to the write a Future<?>.")
public class AsynchronousItemProcessor implements ItemProcessor {
    protected ExecutorService es = null;
    protected ItemProcessor delegate = null;

    @Inject
    @BatchProperty
    @Documentation("the pool size, if <= 0 cached threads are used")
    private String poolSize;

    @Inject
    @BatchProperty
    @Documentation("Locator for the delegate processor")
    private String locator;

    @Inject
    @BatchProperty
    @Documentation("The actual processor (delegate)")
    private String delegateRef;

    protected ExecutorService getExecutor() {
        if (es == null) {
            if (poolSize == null || poolSize.trim().isEmpty()) {
                poolSize = "0";
            }
            final int size = Integer.parseInt(poolSize);
            final DaemonThreadFactory threadFactory = new DaemonThreadFactory();
            if (size <= 0) {
                es = Executors.newCachedThreadPool(threadFactory);
            } else {
                es = Executors.newFixedThreadPool(size, threadFactory);
            }
        }
        return es;
    }

    protected ItemProcessor getDelegate() { // note with cdi delegate scope shouldn't need cleanup
        if (delegate == null) {
            delegate = BeanLocator.Finder.get(locator).newInstance(ItemProcessor.class, delegateRef).getValue();
        }
        return delegate;
    }

    @Override
    public Object processItem(final Object o) throws Exception {
        return getExecutor().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return getDelegate().processItem(o);
            }
        });
    }

    public static class DaemonThreadFactory implements ThreadFactory {
        private static AtomicInteger ids = new AtomicInteger(0);

        private final ThreadGroup group;

        public DaemonThreadFactory() {
            final SecurityManager securityManager = System.getSecurityManager();
            if (securityManager != null) {
                group = securityManager.getThreadGroup();
            } else {
                group = Thread.currentThread().getThreadGroup();
            }
        }

        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = new Thread(group, runnable, getClass().getSimpleName() + " - " + ids.incrementAndGet());
            if (!thread.isDaemon()) {
                thread.setDaemon(true);
            }
            if (thread.getPriority() != Thread.NORM_PRIORITY) {
                thread.setPriority(Thread.NORM_PRIORITY);
            }
            return thread;
        }
    }

}
