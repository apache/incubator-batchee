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
package org.apache.batchee.container.services.locator;

import org.apache.batchee.container.services.ServicesManager;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is not the default since it could create memory leaks
 * But in a server it is quite easy to use setServiceManager/resetServiceManager
 * to handle it (either from the server itself or from an app in lightweight containers).
 * Note: initializeServiceManager can be used instead of setServiceManager to use default behavior
 */
public class ClassLoaderLocator extends SingletonLocator {
    private static final Map<ClassLoader, ServicesManager> MANAGERS = new ConcurrentHashMap<ClassLoader, ServicesManager>();

    public static void setServiceManager(final ClassLoader key, final ServicesManager manager) {
        MANAGERS.put(key, manager);
    }

    public static void initializeServiceManager(final ClassLoader key, final Properties props) {
        final ServicesManager mgr = new ServicesManager();
        final ClassLoader old = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(key);
        try {
            mgr.init(props);
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
        setServiceManager(key, mgr);
    }

    public static ServicesManager resetServiceManager(final ClassLoader key) {
        return MANAGERS.remove(key);
    }

    @Override
    public ServicesManager find() {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl == null) {
            return super.find();
        }

        do {
            final ServicesManager mgr = MANAGERS.get(tccl);
            if (mgr != null) {
                return mgr;
            }
            tccl = tccl.getParent();
        } while (tccl != null);
        return super.find();
    }
}
