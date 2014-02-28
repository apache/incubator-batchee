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
package org.apache.batchee.servlet;

import javax.batch.runtime.Metric;

public final class MetricsHelper {

    private MetricsHelper() {
        // private utility class ct
    }

    public static String toString(final Metric[] metrics) {
        final StringBuilder builder = new StringBuilder();
        final int length = metrics.length;
        if (metrics != null && length > 0) {
            for (int i = 0; i < length; i++) {
                builder.append(metrics[i].getType().name().replace("_COUNT", "")).append(" = ").append(metrics[i].getValue());
                if (i < length - 1) {
                    builder.append(", ");
                }
            }
        }
        return builder.toString();
    }
}
