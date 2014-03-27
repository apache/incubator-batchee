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
package org.apache.batchee.servlet.spi.internal;

import org.apache.batchee.servlet.spi.JobOperatorClassLoaderFinder;
import org.apache.openejb.AppContext;
import org.apache.openejb.loader.SystemInstance;
import org.apache.openejb.spi.ContainerSystem;

import java.util.List;
import java.util.Properties;

// simple impl taking just *the* other app if tomee deploys a single application
// or a specific one if you specifiy in properties batchee.application
//
// side note: it needs the persistence to be shared accross apps (case in a server or with database usage)
public class OpenEJBJobOperatorClassLoaderFinder implements JobOperatorClassLoaderFinder {
    private static final String BATCHEE_APPLICATION = "batchee.application";

    private final ClassLoader ignoreLoader;

    public OpenEJBJobOperatorClassLoaderFinder(final ClassLoader ignoreLoader) {
        this.ignoreLoader = ignoreLoader;
    }

    @Override
    public ClassLoader find(final String name, final Object[] args) {
        final String idToFind = extractId(name, args);

        ClassLoader potentialLoader = null;

        final List<AppContext> appContexts = SystemInstance.get().getComponent(ContainerSystem.class).getAppContexts();
        for (final AppContext app : appContexts) {
            final  String id = app.getId();
            if ("tomee".equals(id) || "openejb".equals(id)) {
                continue;
            }

            potentialLoader = app.getClassLoader();
            if (potentialLoader == ignoreLoader) {
                continue;
            }

            if (id.equals(idToFind)) {
                return potentialLoader;
            }
        }
        return potentialLoader;
    }

    private String extractId(final String name, final Object[] args) {
        if (args != null) {
            for (final Object o : args) {
                if (Properties.class.isInstance(o)) {
                    return Properties.class.cast(o).getProperty(BATCHEE_APPLICATION);
                }
            }
        }
        return null;
    }
}
