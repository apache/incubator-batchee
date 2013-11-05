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
package org.apache.batchee.container.services.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BatcheeThreadFactory implements ThreadFactory {
    public static final ThreadFactory INSTANCE = new BatcheeThreadFactory();

    private static final ThreadGroup GROUP = System.getSecurityManager() != null ? System.getSecurityManager().getThreadGroup() : Thread.currentThread().getThreadGroup();
    private static final String PREFIX = "batchee-thread-";
    private static final AtomicInteger THREAD_NUMBER = new AtomicInteger(1);

    @Override
    public Thread newThread(final Runnable r) {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        // avoid leaks when BatchEE is in the container
        Thread.currentThread().setContextClassLoader(BatcheeThreadFactory.class.getClassLoader());
        try {
            final Thread t = new Thread(GROUP, r, PREFIX + THREAD_NUMBER.getAndIncrement(), 0);
            if (!t.isDaemon()) {
                t.setDaemon(true);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        } finally {
            Thread.currentThread().setContextClassLoader(loader);
        }
    }
}
