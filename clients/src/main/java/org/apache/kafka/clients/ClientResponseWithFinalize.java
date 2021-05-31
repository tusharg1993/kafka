package org.apache.kafka.clients;

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;


public class ClientResponseWithFinalize extends ClientResponse {
  public ClientResponseWithFinalize(RequestHeader requestHeader, RequestCompletionHandler callback, String destination,
      long createdTimeMs, long receivedTimeMs, boolean disconnected, UnsupportedVersionException versionMismatch,
      AuthenticationException authenticationException, AbstractResponse responseBody, MemoryPool memoryPool,
      ByteBuffer responsePayload, LogContext logContext) {
    super(requestHeader, callback, destination, createdTimeMs, receivedTimeMs, disconnected, versionMismatch,
        authenticationException, responseBody, memoryPool, responsePayload, logContext);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    checkBufferRelease();
  }
}
