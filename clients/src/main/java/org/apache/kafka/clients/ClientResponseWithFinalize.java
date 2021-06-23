package org.apache.kafka.clients;

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;


/**
 * This is a decorator for ClientResponse used to verify (at finalization time) that any underlying memory for this
 * response has been returned to the pool. To be used only as a debugging aide.
 */
public class ClientResponseWithFinalize extends ClientResponse {
  public ClientResponseWithFinalize(RequestHeader requestHeader, RequestCompletionHandler callback, String destination,
      long createdTimeMs, long receivedTimeMs, boolean disconnected, UnsupportedVersionException versionMismatch,
      AuthenticationException authenticationException, AbstractResponse responseBody, MemoryPool memoryPool,
      ByteBuffer responsePayload, LogContext logContext) {
    super(requestHeader, callback, destination, createdTimeMs, receivedTimeMs, disconnected, versionMismatch,
        authenticationException, responseBody, memoryPool, responsePayload);
    this.logger = logContext.logger(ClientResponse.class);
  }

  protected void checkAndForceBufferRelease() {
    if (memoryPool != null && responsePayload != null) {
      getLogger().error("ByteBuffer[{}] not released. Ref Count: {}. RequestType: {}",
          responsePayload.position(), refCount.get(), this.requestHeader.apiKey());
      memoryPool.release(responsePayload);
      responsePayload = null;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    checkAndForceBufferRelease();
  }
}
