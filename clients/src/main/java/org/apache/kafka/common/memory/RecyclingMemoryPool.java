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
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of memory pool which recycles buffers of commonly used size.
 * This memory pool is useful if most of the requested buffers' size are within close size range.
 * In this case, instead of deallocate and reallocate the buffer, the memory pool will recycle the buffer for future use.
 */
public class RecyclingMemoryPool implements MemoryPool {
    protected static final Logger log = LoggerFactory.getLogger(RecyclingMemoryPool.class);
    protected final int cacheableBufferSizeUpperThreshold;
    protected final int cacheableBufferSizeLowerThreshold;
    protected final ConcurrentLinkedQueue<ByteBuffer> bufferCacheLarge;

    public RecyclingMemoryPool(int cacheableBufferSizeLowerThreshold, int cacheableBufferSizeUpperThreshold,
        int largeBufferCacheCapacity) {
        this.bufferCacheLarge = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < largeBufferCacheCapacity; ++i) {
            this.bufferCacheLarge.offer(ByteBuffer.allocate(cacheableBufferSizeUpperThreshold));
        }

        this.cacheableBufferSizeUpperThreshold = cacheableBufferSizeUpperThreshold;
        this.cacheableBufferSizeLowerThreshold = cacheableBufferSizeLowerThreshold;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < 1) {
            throw new IllegalArgumentException("requested size " + sizeBytes + "<=0");
        }

        if (sizeBytes <= this.cacheableBufferSizeUpperThreshold
            && sizeBytes >= this.cacheableBufferSizeLowerThreshold) {
            ByteBuffer allocated = this.bufferCacheLarge.poll();
            boolean malloced = false;
            if (allocated != null) {
                allocated.limit(sizeBytes);
            } else {
                malloced = true;
                allocated = ByteBuffer.allocate(sizeBytes);
            }

            if (log.isDebugEnabled()) {
                log.debug("[Size={}] Request[({}] {}.", this.bufferCacheLarge.size(), sizeBytes,
                    malloced ? "malloced" : "fulfilled");
            }

            return allocated;
        } else {
            return ByteBuffer.allocate(sizeBytes);
        }
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null) {
            throw new IllegalArgumentException("provided null buffer");
        }

        if (previouslyAllocated.capacity() == this.cacheableBufferSizeUpperThreshold) {
            previouslyAllocated.clear();
            this.bufferCacheLarge.offer(previouslyAllocated);
        }
    }

    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isOutOfMemory() {
        return false;
    }
}