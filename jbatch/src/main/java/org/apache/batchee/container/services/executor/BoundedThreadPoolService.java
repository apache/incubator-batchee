/*
 * Copyright 2013 International Business Machines Corp.
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

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BoundedThreadPoolService extends AbstractThreadPoolService {
    public static final String BOUNDED_THREADPOOL_MAX_POOL_SIZE = "BOUNDED_THREADPOOL_MAX_POOL_SIZE";
    public static final String BOUNDED_THREADPOOL_MIN_POOL_SIZE = "BOUNDED_THREADPOOL_MIN_POOL_SIZE";
    public static final String BOUNDED_THREADPOOL_KEEP_ALIVE_TIME = "BOUNDED_THREADPOOL_KEEP_ALIVE_TIME";
    public static final String BOUNDED_THREADPOOL_QUEUE_SIZE = "BOUNDED_THREADPOOL_QUEUE_SIZE";

    @Override
    protected ExecutorService newExecutorService(Properties batchConfig) {
        return new ThreadPoolExecutor(
            getInt(batchConfig, BOUNDED_THREADPOOL_MIN_POOL_SIZE, "3"),
            getInt(batchConfig, BOUNDED_THREADPOOL_MAX_POOL_SIZE, "10"),
            getInt(batchConfig, BOUNDED_THREADPOOL_KEEP_ALIVE_TIME, "900"), TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(getInt(batchConfig, BOUNDED_THREADPOOL_QUEUE_SIZE, "4096")),
            BatcheeThreadFactory.INSTANCE);
    }

    private static int getInt(final Properties batchConfig, final String key, final String defaultValue) {
        return Integer.parseInt(batchConfig.getProperty(key, defaultValue));
    }
}
