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
package org.apache.batchee.extras.chain;

import org.apache.batchee.extras.locator.BeanLocator;

import javax.batch.api.Batchlet;

// not sure this is quite useful since a batchlet is a step and you can chain steps but it is another sample of Chain<?>
public class ChainBatchlet extends Chain<Batchlet> implements Batchlet {
    @Override
    public String process() throws Exception {
        return String.class.cast(runChain(null));
    }

    @Override
    public void stop() throws Exception {
        // no-op
    }

    @Override
    protected Object invoke(final BeanLocator.LocatorInstance<Batchlet> next, final Object current) throws Exception {
        return next.getValue().process();
    }
}
