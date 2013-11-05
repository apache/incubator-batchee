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
package org.apache.batchee.shiro;

import org.apache.batchee.container.services.security.DefaultSecurityService;

import java.util.Properties;

import static org.apache.shiro.SecurityUtils.getSubject;

public class ShiroSecurityService extends DefaultSecurityService {
    private String instancePrefix;
    private String permissionPrefix;

    private boolean isAuthenticatedAndAuthorized(final String permission) {
        return getSubject().isAuthenticated() && getSubject().isPermitted(permission);
    }

    @Override
    public boolean isAuthorized(final long instanceId) {
        return isAuthenticatedAndAuthorized(instancePrefix + instanceId);
    }

    @Override
    public boolean isAuthorized(final String perm) {
        return isAuthenticatedAndAuthorized(permissionPrefix + perm);
    }

    @Override
    public String getLoggedUser() {
        if (getSubject().isAuthenticated()) {
            return getSubject().getPrincipal().toString();
        }
        return super.getLoggedUser();
    }

    @Override
    public void init(final Properties batchConfig) {
        super.init(batchConfig);
        permissionPrefix = batchConfig.getProperty("security.job.permission-prefix", "jbatch:");
        instancePrefix = permissionPrefix + batchConfig.getProperty("security.job.permission-prefix", "instance:");
    }
}
