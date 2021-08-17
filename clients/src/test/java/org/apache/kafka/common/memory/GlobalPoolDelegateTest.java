/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;


public class GlobalPoolDelegateTest {
    @Test(expected = IllegalStateException.class)
    public void testMissingPoolInstance() {
        GlobalPoolDelegate globalPoolDelegate = new GlobalPoolDelegate();
        globalPoolDelegate.configure(Collections.emptyMap());
    }

    @Test
    public void testDelegateCalls() {
        MemoryPool mockMemoryPool = EasyMock.mock(MemoryPool.class);
        EasyMock.expect(mockMemoryPool.availableMemory()).andReturn((long) 100).once();
        EasyMock.expect(mockMemoryPool.isOutOfMemory()).andReturn(false).once();
        EasyMock.expect(mockMemoryPool.size()).andReturn((long) 200).once();
        ByteBuffer byteBuf = ByteBuffer.allocate(0);
        EasyMock.expect(mockMemoryPool.tryAllocate(EasyMock.anyInt())).andReturn(byteBuf).once();
        mockMemoryPool.release(EasyMock.anyObject());
        EasyMock.expectLastCall().once();
        EasyMock.replay(mockMemoryPool);

        GlobalPoolDelegate globalPoolDelegate = new GlobalPoolDelegate();
        globalPoolDelegate.configure(
            Collections.singletonMap("linkedin.memorypool.pool.instance", mockMemoryPool));

        Assert.assertEquals(globalPoolDelegate.availableMemory(), 100);
        Assert.assertEquals(globalPoolDelegate.isOutOfMemory(), false);
        Assert.assertEquals(globalPoolDelegate.size(), 200);
        Assert.assertEquals(globalPoolDelegate.tryAllocate(0), byteBuf);
        globalPoolDelegate.release(byteBuf);

        EasyMock.verify(mockMemoryPool);
    }
}
