/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Telemetry rejection reason codes.
 * These codes are used in TelemetryRejectedPayload.reasonCode field.
 */
export const REJECTION_CODES = {
  /** Timestamp is outside the allowed clock skew window */
  STALE_TIMESTAMP: 1,

  /** Sensor is not registered or has been disabled */
  SENSOR_FORBIDDEN: 2,

  /** Nonce has already been used by this sensor (replay attack prevention) */
  DUPLICATE_NONCE: 3,

  /** Ed25519 signature verification failed */
  INVALID_SIGNATURE: 4,

  /** Kafka publish operation failed after retries */
  KAFKA_PUBLISH_FAILED: 999,
} as const;

export type RejectionCode =
  (typeof REJECTION_CODES)[keyof typeof REJECTION_CODES];

/**
 * Get human-readable description for a rejection code.
 */
export function getRejectionCodeDescription(code: number): string {
  switch (code) {
    case REJECTION_CODES.STALE_TIMESTAMP:
      return 'Timestamp outside allowed skew window';
    case REJECTION_CODES.SENSOR_FORBIDDEN:
      return 'Sensor is unknown or disabled';
    case REJECTION_CODES.DUPLICATE_NONCE:
      return 'Nonce already used for this sensor';
    case REJECTION_CODES.INVALID_SIGNATURE:
      return 'Signature verification failed';
    case REJECTION_CODES.KAFKA_PUBLISH_FAILED:
      return 'Kafka publish failed';
    default:
      return `Unknown rejection code: ${code}`;
  }
}
