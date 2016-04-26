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

import javax.batch.api.chunk.CheckpointAlgorithm;

public final class ItemCheckpointAlgorithm implements CheckpointAlgorithm {
    private long checkpointBeginTime = 0;
    private int itemsRead = 0;

    private int time;
    private int item;

    public void setItemCount(int itemCount) {
        this.item = itemCount;
    }

    public void setTimeLimitSeconds(int timeLimitSeconds) {
        this.time = timeLimitSeconds;
    }

    @Override
    public void endCheckpoint() throws Exception {
        // no-op
    }

    public boolean isReadyToCheckpointItem() throws Exception {
        return (itemsRead >= item);
    }

    public boolean isReadyToCheckpointTime() throws Exception {
        boolean timeready = false;
        final long currentTime = System.currentTimeMillis();
        final long curdiff = currentTime - checkpointBeginTime;
        final int diff = (int) curdiff / 1000;

        if (diff >= time) {
            timeready = true;

            checkpointBeginTime = System.currentTimeMillis();

        }

        return timeready;
    }

    @Override
    public boolean isReadyToCheckpoint() throws Exception {
        boolean ready = false;

        itemsRead++;

        if (time == 0) { // no time limit, just check if item count has been reached
            if (isReadyToCheckpointItem()) {
                ready = true;
            }
        } else if (isReadyToCheckpointItem() || isReadyToCheckpointTime()) {
            ready = true;
        }

        return ready;
    }

    @Override
    public void beginCheckpoint() throws Exception {
        checkpointBeginTime = System.currentTimeMillis();
        itemsRead = 0;
    }

    @Override
    public int checkpointTimeout() throws Exception {
        return 0;
    }
}
