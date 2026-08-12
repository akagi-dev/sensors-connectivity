import { describe, expect, it } from 'vitest';
import { nextKafkaOffset } from '../src/kafka-offset.js';

describe('Kafka batch commit offset', () => {
  it('commits the offset immediately after the final batch message', () => {
    expect(nextKafkaOffset('8')).toBe('9');
    expect(nextKafkaOffset('9223372036854775807')).toBe('9223372036854775808');
  });

  it('rejects a malformed offset', () => {
    expect(() => nextKafkaOffset('-1')).toThrow('Invalid Kafka offset');
    expect(() => nextKafkaOffset('1.5')).toThrow('Invalid Kafka offset');
  });
});
