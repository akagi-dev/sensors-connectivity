/** Calculates the next Kafka offset without losing 64-bit precision. */
export function nextKafkaOffset(offset: string): string {
  if (!/^\d+$/.test(offset)) {
    throw new Error(`Invalid Kafka offset: ${offset}`);
  }
  return (BigInt(offset) + 1n).toString();
}
