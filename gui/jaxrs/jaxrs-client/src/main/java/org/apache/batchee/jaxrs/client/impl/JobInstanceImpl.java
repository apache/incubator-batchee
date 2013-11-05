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
package org.apache.batchee.jaxrs.client.impl;

import javax.batch.runtime.JobInstance;

public class JobInstanceImpl implements JobInstance {
    private final String name;
    private final long id;

    public JobInstanceImpl(final String jobName, final long instanceId) {
        this.name = jobName;
        this.id = instanceId;
    }

    @Override
    public long getInstanceId() {
        return id;
    }

    @Override
    public String getJobName() {
        return name;
    }
}
