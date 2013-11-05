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
package org.apache.batchee.container.services.security;

import org.apache.batchee.spi.SecurityService;

import java.util.Properties;

public class DefaultSecurityService implements SecurityService {
    protected String defaultUser;
    protected boolean allowDefault;

    @Override
    public boolean isAuthorized(final long instanceId) {
        return isDefaultUserAuthorized();
    }

    @Override
    public boolean isAuthorized(final String perm) {
        return isDefaultUserAuthorized();
    }

    @Override
    public String getLoggedUser() {
        return defaultUser;
    }

    private boolean isDefaultUserAuthorized() {
        return defaultUser.equals(getLoggedUser()) || allowDefault;
    }

    @Override
    public void init(final Properties batchConfig) {
        defaultUser = batchConfig.getProperty("security.user", "jbatch");
        allowDefault = "true".equalsIgnoreCase(batchConfig.getProperty("security.anonymous-allowed", "false"));
    }
}
