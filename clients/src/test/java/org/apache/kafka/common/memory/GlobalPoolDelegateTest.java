package org.apache.kafka.common.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.kafka.clients.CommonClientConfigs;
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
            Collections.singletonMap(CommonClientConfigs.POOL_INSTANCE_CONFIG, mockMemoryPool));

        Assert.assertEquals(globalPoolDelegate.availableMemory(), 100);
        Assert.assertEquals(globalPoolDelegate.isOutOfMemory(), false);
        Assert.assertEquals(globalPoolDelegate.size(), 200);
        Assert.assertEquals(globalPoolDelegate.tryAllocate(0), byteBuf);
        globalPoolDelegate.release(byteBuf);

        EasyMock.verify(mockMemoryPool);
    }
}
