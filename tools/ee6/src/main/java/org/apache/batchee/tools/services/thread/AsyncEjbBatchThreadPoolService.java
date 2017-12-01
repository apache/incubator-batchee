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
package org.apache.batchee.tools.services.thread;

import java.util.Properties;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.apache.batchee.container.cdi.BatchCDIInjectionExtension;
import org.apache.batchee.spi.BatchThreadPoolService;

/**
 * This is an implementation of a {@link org.apache.batchee.spi.BatchThreadPoolService}
 * which uses an &#64;Asynchronous EJB method to spawn new Threads.
 * The main reason for doing this is to have properly setup JavaEE Threads even
 * in JavaEE 6 environments where BatchEE is not deeply integrated in other ways.
 *
 * Activate this class in a batchee.properties files as
 * <pre>
 * BatchThreadPoolService=org.apache.batchee.tools.services.thread.AsyncEjbBatchThreadPoolService
 * </pre>
 *
 * For some containers you might additionally need to enable the
 * {@link org.apache.batchee.tools.services.thread.UserTransactionTransactionService} for
 * proper JTA transaction handling.
 *
 */
public class AsyncEjbBatchThreadPoolService implements BatchThreadPoolService {
    
    private BeanManager beanManager;
    private ThreadExecutorEjb threadExecutorEjb;
    
    @Override
    public void init(Properties batchConfig) {
        beanManager = BatchCDIInjectionExtension.getInstance().getBeanManager();
        
        Set<Bean<?>> beans = beanManager.getBeans(ThreadExecutorEjb.class);
        Bean<?> bean = beanManager.resolve(beans);
        CreationalContext cc = beanManager.createCreationalContext(bean);
        
        threadExecutorEjb = (ThreadExecutorEjb) beanManager.getReference(bean, bean.getBeanClass(), cc);
    }
    
    @Override
    public void executeTask(Runnable work, Object config) {
        threadExecutorEjb.executeTask(work, config);
    }
    
    @Override
    public void shutdown() {
        // We cannot force an async EJB to shutdown.
        // This usually works out of the box if the container EJB
        // undeploys or stops the application.
    }
}
