/* Copyright 2017 Sabre Holdings */
package org.example.ym.concbuff;

import java.time.Instant;

class MessageWithTimestamp {

    private final Message message;
    public final Instant timestamp;

    MessageWithTimestamp(Message message, Instant timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    public static MessageWithTimestamp binarySearchTimestamp(Instant timestamp) {
        return new MessageWithTimestamp(null, timestamp);
    }

    public Message getMessage() {
        return message;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "MessageWithTimestamp{" +
                "message=" + message +
                ", timestamp=" + String.valueOf((timestamp.getEpochSecond())) + " sec " + String.valueOf((timestamp.toEpochMilli() % 1000)) + " millis" +
                '}';
    }
}
