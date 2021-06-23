package org.apache.kafka.clients;

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;


/**
 * This is a decorator for ClientResponse used to verify (at finalization time) that any underlying memory for this
 * response has been returned to the pool. To be used only as a debugging aide.
 */
public class ClientResponseWithFinalize extends ClientResponse {
  private final LogContext logContext;

  public ClientResponseWithFinalize(RequestHeader requestHeader, RequestCompletionHandler callback, String destination,
      long createdTimeMs, long receivedTimeMs, boolean disconnected, UnsupportedVersionException versionMismatch,
      AuthenticationException authenticationException, AbstractResponse responseBody, MemoryPool memoryPool,
      ByteBuffer responsePayload, LogContext logContext) {
    super(requestHeader, callback, destination, createdTimeMs, receivedTimeMs, disconnected, versionMismatch,
        authenticationException, responseBody, memoryPool, responsePayload);
    this.logContext = logContext;
  }

  private Logger getLogger() {
    if (logContext != null) {
      return logContext.logger(ClientResponseWithFinalize.class);
    }
    return logger;
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
