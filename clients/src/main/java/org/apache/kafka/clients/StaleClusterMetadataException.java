package org.apache.kafka.clients;

import org.apache.kafka.common.errors.InvalidMetadataException;

/**
 * Thrown when current metadata is from a stale cluster that has a different cluster id
 * than original cluster.
 * Note: this is not a public API.
 */
public class StaleClusterMetadataException extends InvalidMetadataException {
  private static final long serialVersionUID = 1L;

  public StaleClusterMetadataException() {}

  public StaleClusterMetadataException(String message) {
    super(message);
  }
}
