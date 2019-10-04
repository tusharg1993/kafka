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
package org.apache.kafka.common.network;

import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.types.Struct;


/**
 * A size delimited Send that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class StructSend extends ByteBufferSend {

    public StructSend(String destination, Struct header, Struct body) {
        super(destination, serializeSizeDelimit(header, body));
    }

    private static ByteBuffer[] serializeSizeDelimit(Struct header, Struct body) {
        ByteBuffer headerBuffer = header.getSerialized();
        if (headerBuffer == null) {
            headerBuffer = ByteBuffer.allocate(header.sizeOf());
            header.writeTo(headerBuffer);
        }

        ByteBuffer bodyBuffer = body.getSerialized();
        if (bodyBuffer == null) {
            bodyBuffer = ByteBuffer.allocate(body.sizeOf());
            body.writeTo(bodyBuffer);
        }

        headerBuffer.rewind();
        bodyBuffer.rewind();
        return sizeDelimit(headerBuffer, bodyBuffer);
    }

    private static ByteBuffer[] sizeDelimit(ByteBuffer headerBuffer, ByteBuffer bodyBuffer) {
        return new ByteBuffer[] {
            // Possible INT_OVERFLOW? Ignoring for now since header is presumed to be very small.
            sizeBuffer(headerBuffer.remaining() + bodyBuffer.remaining()),
            headerBuffer,
            bodyBuffer
        };
    }

    private static ByteBuffer sizeBuffer(int size) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(size);
        sizeBuffer.rewind();
        return sizeBuffer;
    }
}
