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
package org.apache.batchee.jaxrs.common;

import javax.batch.runtime.JobInstance;
import java.util.ArrayList;
import java.util.List;

public class RestJobInstance {
    private String name;
    private long id;

    public RestJobInstance() {
        // no-op
    }

    public RestJobInstance(final String name, final long id) {
        this.name = name;
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setId(final long id) {
        this.id = id;
    }

    public static List<RestJobInstance> wrap(final List<JobInstance> jobInstances) {
        final List<RestJobInstance> instances = new ArrayList<RestJobInstance>(jobInstances.size());
        for (final JobInstance instance : jobInstances) {
            instances.add(wrap(instance));
        }
        return instances;
    }

    public static RestJobInstance wrap(final JobInstance instance) {
        return new RestJobInstance(instance.getJobName(), instance.getInstanceId());
    }
}
