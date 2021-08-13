package org.apache.kafka.common.memory;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Configurable;


public class GlobalPoolDelegate implements MemoryPool, Configurable {
  private MemoryPool delegateMemoryPool;

  @Override
  public void configure(Map<String, ?> configs) {
    if (!configs.containsKey(CommonClientConfigs.POOL_INSTANCE_CONFIG)) {
      throw new IllegalStateException(CommonClientConfigs.POOL_INSTANCE_CONFIG + " not found in configs while configuring "
          + "GlobalPoolDelegate object.");
    }
    delegateMemoryPool = (MemoryPool) configs.get(CommonClientConfigs.POOL_INSTANCE_CONFIG);
  }

  @Override
  public ByteBuffer tryAllocate(int sizeBytes) {
    return delegateMemoryPool.tryAllocate(sizeBytes);
  }

  @Override
  public void release(ByteBuffer previouslyAllocated) {
    delegateMemoryPool.release(previouslyAllocated);
  }

  @Override
  public long size() {
    return delegateMemoryPool.size();
  }

  @Override
  public long availableMemory() {
    return delegateMemoryPool.availableMemory();
  }

  @Override
  public boolean isOutOfMemory() {
    return delegateMemoryPool.isOutOfMemory();
  }
}
