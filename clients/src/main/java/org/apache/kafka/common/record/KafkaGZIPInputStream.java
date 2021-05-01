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
package org.apache.kafka.common.record;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;


public class KafkaGZIPInputStream extends GZIPInputStream {
  protected final BufferSupplier bufferSupplier;
  protected final ByteBuffer byteBuffer;

  private boolean closed = false;

  public KafkaGZIPInputStream(InputStream in, int size, BufferSupplier bufferSupplier) throws IOException {
    super(in, 0);
    this.bufferSupplier = bufferSupplier;
    this.byteBuffer = bufferSupplier.get(size);
    if (!byteBuffer.hasArray() || byteBuffer.arrayOffset() != 0) {
      throw new RuntimeException("buffer must have backing array with zero array offset");
    }
    buf = byteBuffer.array();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      super.close();
      this.bufferSupplier.release(byteBuffer);
      closed = true;
    }
  }
}
