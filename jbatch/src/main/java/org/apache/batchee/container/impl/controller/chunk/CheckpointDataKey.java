/*
 * Copyright 2012 International Business Machines Corp.
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
package org.apache.batchee.container.impl.controller.chunk;

public class CheckpointDataKey {
    private long jobInstanceId;
    private CheckpointType type;
    private String stepName;

    public CheckpointDataKey(final long jobId, final String stepName, final CheckpointType bdsName) {
        this.jobInstanceId = jobId;
        this.stepName = stepName;
        this.type = bdsName;
    }

    public long getJobInstanceId() {
        return jobInstanceId;
    }

    public CheckpointType getType() {
        return type;
    }

    public String getStepName() {
        return stepName;
    }

    public String getCommaSeparatedKey() {
        return jobInstanceId + "," + stepName + "," + type.name();
    }

    @Override
    public String toString() {
        return getCommaSeparatedKey();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CheckpointDataKey that = CheckpointDataKey.class.cast(o);
        return jobInstanceId == that.jobInstanceId
            && type.equals(that.type)
            && stepName.equals(that.stepName);
    }

    @Override
    public int hashCode() {
        int result = (int) (jobInstanceId ^ (jobInstanceId >>> 32));
        result = 31 * result + type.hashCode();
        result = 31 * result + stepName.hashCode();
        return result;
    }
}
