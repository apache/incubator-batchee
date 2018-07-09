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
package org.apache.batchee.cli.lifecycle.impl;

import org.apache.batchee.cli.classloader.ChildFirstURLClassLoader;
import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.spi.BatchThreadPoolService;
import org.apache.openejb.OpenEjbContainer;
import org.apache.openejb.UndeployException;
import org.apache.openejb.assembler.classic.AppInfo;
import org.apache.openejb.assembler.classic.Assembler;
import org.apache.openejb.config.AppModule;
import org.apache.openejb.config.ConfigurationFactory;
import org.apache.openejb.config.DeploymentFilterable;
import org.apache.openejb.config.DeploymentLoader;
import org.apache.openejb.core.LocalInitialContext;
import org.apache.openejb.core.LocalInitialContextFactory;
import org.apache.openejb.loader.SystemInstance;

import javax.ejb.embeddable.EJBContainer;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

// EJBContainer doesn't support war deployment
public class OpenEJBLifecycle extends LifecycleBase<Object> {
    private Assembler assembler = null;
    private AppInfo info = null;

    private AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public Object start() {
        final Map<String, Object> config = configuration("openejb");

        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (ChildFirstURLClassLoader.class.isInstance(loader)) {
            final File app = ChildFirstURLClassLoader.class.cast(loader).getApplicationFolder();
            if (app != null) {
                if (new File(app, "WEB-INF").exists()) {
                    // put default to let the container start
                    final ConcurrentMap<String,Object> configuration = new ConcurrentHashMap<String, Object>(config);
                    configuration.putIfAbsent(Context.INITIAL_CONTEXT_FACTORY, LocalInitialContextFactory.class.getName());
                    configuration.putIfAbsent(LocalInitialContext.ON_CLOSE, LocalInitialContext.Close.DESTROY.name());
                    configuration.putIfAbsent(DeploymentFilterable.DEPLOYMENTS_CLASSPATH_PROPERTY, "false");
                    try {
                        LocalInitialContextFactory.class.getClassLoader().loadClass("org.apache.openejb.server.ServiceManager");
                        configuration.putIfAbsent(OpenEjbContainer.OPENEJB_EMBEDDED_REMOTABLE, "true");
                    } catch (final Exception e) {
                        // ignored
                    }

                    // convert to props since we need hashtable :(
                    final Properties props = new Properties();
                    props.putAll(configuration);

                    try {
                        final InitialContext initialContext = new InitialContext(props);

                        final AppModule appModule = new DeploymentLoader().load(app);
                        info = new ConfigurationFactory().configureApplication(appModule);
                        assembler = SystemInstance.get().getComponent(Assembler.class);
                        assembler.createApplication(info, loader);

                        running.set(true);
                        return initialContext;
                    } catch (final Exception e) {
                        throw new BatchContainerRuntimeException(e);
                    }
                } else {
                    throw new BatchContainerRuntimeException("bar not supported by OpenEJB, use a war please");
                }
            }
        }

        // ensure you don't put folder in loader
        return EJBContainer.createEJBContainer(config);
    }

    @Override
    public void stop(final Object state) {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        BatchThreadPoolService threadPoolService = ServicesManager.find().service(BatchThreadPoolService.class);
        threadPoolService.shutdown();

        if (state != null) {
            if (assembler != null && info != null) {
                try {
                    assembler.destroyApplication(info);
                } catch (final UndeployException e) {
                    throw new BatchContainerRuntimeException(e);
                }
            }

            try {
                state.getClass().getMethod("close").invoke(state);
            } catch (final IllegalAccessException e) {
                // no-op
            } catch (final InvocationTargetException e) {
                // no-op
            } catch (final NoSuchMethodException e) {
                // no-op
            }
        }
    }

    public static void main(String[] args) {
        EJBContainer.createEJBContainer();
    }
}
