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

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.services.ServicesManagerLocator;
import org.apache.batchee.container.services.factory.DefaultBatchArtifactFactory;
import org.apache.batchee.spi.BatchArtifactFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;

public class SpringLifecycle extends LifecycleBase<AbstractApplicationContext> {
    @Override
    public AbstractApplicationContext start() {
        final Map<String, Object> config = configuration("spring");

        final AbstractApplicationContext ctx;
        if (config.containsKey("locations")) {
            final Collection<String> locations = new LinkedList<String>();
            locations.addAll(asList(config.get("locations").toString().split(",")));
            ctx = new ClassPathXmlApplicationContext(locations.toArray(new String[locations.size()]));
        } else if (config.containsKey("classes")) {
            final Collection<Class<?>> classes = new LinkedList<Class<?>>();
            final ClassLoader loader = currentThread().getContextClassLoader();
            for (final String clazz : config.get("classes").toString().split(",")) {
                try {
                    classes.add(loader.loadClass(clazz));
                } catch (final ClassNotFoundException e) {
                    throw new BatchContainerRuntimeException(e);
                }
            }
            ctx = new AnnotationConfigApplicationContext(classes.toArray(new Class<?>[classes.size()]));
        } else {
            throw new IllegalArgumentException(LIFECYCLE_PROPERTIES_FILE + " should contain 'classes' or 'locations' key");
        }

        final Properties p = new Properties();
        p.put(BatchArtifactFactory.class.getName(), new SpringArtifactFactory(ctx));

        final ServicesManager mgr = new ServicesManager();
        mgr.init(p);

        ServicesManager.setServicesManagerLocator(new ServicesManagerLocator() {
            @Override
            public ServicesManager find() {
                return mgr;
            }
        });

        return ctx;
    }

    @Override
    public void stop(final AbstractApplicationContext state) {
        state.close();
    }

    public static class SpringArtifactFactory extends DefaultBatchArtifactFactory {
        private final ApplicationContext context;

        public SpringArtifactFactory(final ApplicationContext ctx) {
            context = ctx;
        }

        @Override
        protected ArtifactLocator createArtifactsLocator(final ClassLoader tccl) {
            return new SpringArtifactLocator(super.createArtifactsLocator(tccl), context);
        }

        private static class SpringArtifactLocator implements ArtifactLocator {
            private final ArtifactLocator parent;
            private final ApplicationContext context;

            public SpringArtifactLocator(final ArtifactLocator parent, final ApplicationContext context) {
                this.parent = parent;
                this.context = context;
            }

            @Override
            public Object getArtifactById(final String id) {
                final Object bean = context.getBean(id);
                if (bean != null) {
                    return bean;
                }
                return parent.getArtifactById(id);
            }
        }
    }
}
