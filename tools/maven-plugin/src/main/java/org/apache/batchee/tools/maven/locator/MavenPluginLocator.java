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
package org.apache.batchee.tools.maven.locator;

import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.services.ServicesManagerLocator;

import java.util.Map;
import java.util.Properties;

public class MavenPluginLocator implements ServicesManagerLocator {
    private static final String BATCHEE_VERBOSE = "org.apache.batchee.init.verbose";

    private ServicesManager manager;

    @Override
    public ServicesManager find() {
        return manager;
    }

    public void init(final Map<String, String> properties) {
        manager = new ServicesManager();

        final Properties config = new Properties();
        if (properties != null) {
            config.putAll(properties);
        }
        if (!config.containsKey(BATCHEE_VERBOSE)) {
            config.setProperty(BATCHEE_VERBOSE, "false"); // we don't want BatchEE banner in mvn
        }
        manager.init(config);
    }
}
