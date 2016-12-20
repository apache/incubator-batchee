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
package org.apache.batchee.cdi.partitioned;

import org.apache.batchee.cdi.component.JobScopedBean;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.AbstractItemReader;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.logging.Level;
import java.util.logging.Logger;

@Named
@Dependent
public class PartitionedJobScopedReader extends AbstractItemReader {

    private static final Logger LOG = Logger.getLogger(PartitionedJobScopedReader.class.getName());

    private static volatile boolean stop;
    private static volatile boolean stopPartition2;

    private static long originalBeanId;
    private static long currentBeanId;

    private boolean firstRun = true;

    @Inject
    @BatchProperty
    private Integer partition;

    @Inject
    private JobScopedBean jobScopedBean;


    @Override
    public Object readItem() throws Exception {

        if (firstRun) {
            originalBeanId = jobScopedBean.getId();
        }

        currentBeanId = jobScopedBean.getId();

        if (partition == 2 && stopPartition2 || partition != 2 && stop) {
            LOG.log(Level.INFO, "Finished partition "+  partition);
            return null;
        }

        Thread.sleep(10);
        return "Partition " + partition + ": JobScopedBean destroyed? " + JobScopedBean.isDestroyed();
    }


    public static void stop() {
        LOG.log(Level.INFO, "Stopping all partitions except Partition 2");
        stop = true;
    }

    public static void stopPartition2() {
        stopPartition2 = true;
    }

    public static long originalBeanId() {
        return originalBeanId;
    }

    public static long currentBeanId() {
        return currentBeanId;
    }
}
