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

import javax.batch.runtime.Metric;

public class RestMetric {
    private Metric.MetricType type;
    private long value;

    public RestMetric() {
        // no-op
    }

    public RestMetric(final Metric.MetricType type, final long value) {
        this.type = type;
        this.value = value;
    }

    public void setType(final Metric.MetricType type) {
        this.type = type;
    }

    public void setValue(final long value) {
        this.value = value;
    }

    public Metric.MetricType getType() {
        return type;
    }

    public long getValue() {
        return value;
    }

    public static RestMetric wrap(final Metric m) {
        return new RestMetric(m.getType(), m.getValue());
    }
}
