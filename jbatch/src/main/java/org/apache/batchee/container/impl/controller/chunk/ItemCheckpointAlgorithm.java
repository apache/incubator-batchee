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
    private long requests = 0;
    private long checkpointBeginTime = 0;

    private int time;
    private int item;
    private long currentTime;

    public ItemCheckpointAlgorithm() {
        checkpointBeginTime = System.currentTimeMillis();
        currentTime = checkpointBeginTime;
    }

    @Override
    public void endCheckpoint() throws Exception {
        // no-op
    }

    public boolean isReadyToCheckpointItem() throws Exception {
        requests++;

        final boolean itemready = (requests >= item);
        if (itemready) {
            requests = 0;
        }
        return itemready;
    }

    public boolean isReadyToCheckpointTime() throws Exception {
        boolean timeready = false;
        currentTime = System.currentTimeMillis();
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

        if (time == 0) { // no time limit, just check if item count has been reached
            if (isReadyToCheckpointItem()) {
                ready = true;
            }
        } else if (isReadyToCheckpointItem() || isReadyToCheckpointTime()) {
            ready = true;
        }

        return ready;
    }

    public void setThresholds(int itemthreshold, int timethreshold) {
        item = itemthreshold;
        time = timethreshold;
    }

    @Override
    public void beginCheckpoint() throws Exception {
        checkpointBeginTime = currentTime;
    }

    @Override
    public int checkpointTimeout() throws Exception {
        return 0;
    }
}
