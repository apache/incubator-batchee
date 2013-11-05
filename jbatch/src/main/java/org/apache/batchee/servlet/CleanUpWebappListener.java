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
package org.apache.batchee.servlet;

import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.jmx.BatchEEMBean;
import org.apache.batchee.spi.BatchThreadPoolService;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.lang.management.ManagementFactory;

@WebListener
public class CleanUpWebappListener implements ServletContextListener {
    @Override
    public void contextInitialized(final ServletContextEvent sce) {
        // no-op
    }

    @Override
    public void contextDestroyed(final ServletContextEvent sce) {
        final BatchThreadPoolService threadPoolService = ServicesManager.service(BatchThreadPoolService.class);
        if (CleanUpWebappListener.class.getClassLoader() == sce.getServletContext().getClassLoader()) {
            threadPoolService.shutdown();

            // unregister jmx bean if deployed in an app
            final MBeanServer jmx = ManagementFactory.getPlatformMBeanServer();
            try {
                final ObjectName objectName = new ObjectName(BatchEEMBean.DEFAULT_OBJECT_NAME);
                if (jmx.isRegistered(objectName)) {
                    jmx.unregisterMBean(objectName);
                }
            } catch (final Exception e) {
                // no-op
            }
        }
    }
}
