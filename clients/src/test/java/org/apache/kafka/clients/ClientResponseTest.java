package org.apache.kafka.clients;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ClientResponseTest {

  private ClientResponse _clientResponse;
  private ClientResponse _clientResponseNoPooling;
  private ClientResponse _clientResponseDisconnected;
  private MemoryPool _memoryPool;

  @Before
  public void setup() {
    RequestHeader requestHeader = new RequestHeader(ApiKeys.FETCH, (short) 1, "someclient", 1);
    NetworkClientTest.TestCallbackHandler callbackHandler = new NetworkClientTest.TestCallbackHandler();
    FetchResponse fetchResponse = new FetchResponse(Errors.NONE, new LinkedHashMap<>(), 0, 1);
    _memoryPool = EasyMock.createMock(MemoryPool.class);

    _clientResponse =
        new ClientResponse(requestHeader, callbackHandler, "node0", 100, 110, false, null, null, fetchResponse,
            _memoryPool, ByteBuffer.allocate(1));
    _clientResponseNoPooling =
        new ClientResponse(requestHeader, callbackHandler, "node0", 100, 110, false, null, null, fetchResponse,
            MemoryPool.NONE, ByteBuffer.allocate(1));
    _clientResponseDisconnected =
        new ClientResponse(requestHeader, callbackHandler, "node0", 100, 110, true, null, null, fetchResponse);
  }

  private void decrementRefCountBelowZero(ClientResponse clientResponse) {
    clientResponse.decRefCount();
  }

  private void incrementRefCountAfterBufferRelease(ClientResponse clientResponse) {
    clientResponse.incRefCount();
    clientResponse.decRefCount();

    // Buffer released now. Try incrementing the ref count again.
    clientResponse.incRefCount();
  }

  @Test
  public void testClientResponseBufferRelease() {
    _memoryPool.release(EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(_memoryPool);

    _clientResponse.incRefCount();
    _clientResponse.decRefCount();

    EasyMock.verify(_memoryPool);
  }

  @Test
  public void testDecRefCountBelowZeroMemoryPoolNone() {
    try {
      decrementRefCountBelowZero(_clientResponseNoPooling);
    } catch (Exception e) {
      Assert.fail("Client response with MemoryPool.NONE should not throw.");
    }
  }

  @Test
  public void testDecRefCountBelowZeroDisconnected() {
    try {
      decrementRefCountBelowZero(_clientResponseDisconnected);
    } catch (Exception e) {
      Assert.fail("Client response with disconnection should not throw.");
    }
  }

  @Test
  public void testDecRefCountBelowZeroShouldThrow() {
    try {
      decrementRefCountBelowZero(_clientResponse);
      Assert.fail("Decrementing ref count below zero should throw.");
    } catch (Exception e) {
      Assert.assertEquals(IllegalStateException.class, e.getClass());
      Assert.assertEquals("Ref count decremented below zero. This should never happen.", e.getMessage());
    }
  }

  @Test
  public void testIncRefCountAfterBufferReleaseMemoryPoolNone() {
    try {
      incrementRefCountAfterBufferRelease(_clientResponseNoPooling);
    } catch (Exception e) {
      Assert.fail("Client response with MemoryPool.NONE should not throw.");
    }
  }

  @Test
  public void testIncRefCountAfterBufferReleaseDisconnected() {
    try {
      incrementRefCountAfterBufferRelease(_clientResponseDisconnected);
    } catch (Exception e) {
      Assert.fail("Client response with disconnection should not throw.");
    }
  }

  @Test
  public void testIncRefCountAfterBufferReleaseShouldThrow() {
    try {
      incrementRefCountAfterBufferRelease(_clientResponse);
      Assert.fail("Incrementing ref count after releasing pool should throw.");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Ref count being incremented again after buffer release. This should never happen.",
          e.getMessage());
    }
  }
}
