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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

public class CheckpointData implements Serializable {
    private static final long serialVersionUID = 1L;
    private long _jobInstanceId;
    private CheckpointType type;
    private String stepName;
    private byte[] restartToken;

    public CheckpointData(final long jobInstanceId, final String stepname, final CheckpointType ckType) {
        if (stepname != null && ckType != null) {
            _jobInstanceId = jobInstanceId;
            type = ckType;
            stepName = stepname;
            try {
                restartToken = "NOTSET".getBytes("UTF8");
            } catch (final UnsupportedEncodingException e) {
                throw new RuntimeException("Doesn't support UTF-8", e);
            }
        } else {
            throw new RuntimeException("Invalid parameters to CheckpointData jobInstanceId: " + _jobInstanceId +
                " BDS: " + ckType + " stepName: " + stepname);
        }
    }

    public long getjobInstanceId() {
        return _jobInstanceId;
    }

    public void setjobInstanceId(final long id) {
        _jobInstanceId = id;
    }

    public CheckpointType getType() {
        return type;
    }

    public void setType(final CheckpointType ckType) {
        type = ckType;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(final String name) {
        stepName = name;
    }

    public byte[] getRestartToken() {
        return restartToken;
    }

    public void setRestartToken(byte[] token) {
        restartToken = token;
    }

    @Override
    public String toString() {
        String restartString;
        try {
            restartString = new String(this.restartToken, "UTF8");
        } catch (UnsupportedEncodingException e) {
            restartString = "<bytes not UTF-8>";
        }
        return " jobInstanceId: " + _jobInstanceId + " stepId: " + this.stepName + " bdsName: " + this.type.name() +
            " restartToken: [UTF8-bytes: " + restartString;
    }
}

