/**
 * Copyright 2013 International Business Machines Corp.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.container.services;

import org.apache.batchee.container.impl.JobContextImpl;

import javax.batch.runtime.JobExecution;
import java.sql.Timestamp;
import java.util.Properties;

public interface InternalJobExecution extends JobExecution {
    void setBatchStatus(String status);
    void setCreateTime(Timestamp ts);
    void setEndTime(Timestamp ts);
    void setExitStatus(String status);
    void setLastUpdateTime(Timestamp ts);
    void setStartTime(Timestamp ts);
    void setJobParameters(Properties jProps);
    long getInstanceId();
    void setJobContext(JobContextImpl jobContext);
}
