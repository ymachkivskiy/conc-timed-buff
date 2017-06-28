/* Copyright 2017 Sabre Holdings */
package org.example.ym.concbuff;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;

class MessageWithTimestamp {

    private static final Comparator<MessageWithTimestamp> oneMilliGranularityComparator = comparing(
            MessageWithTimestamp::getTimestamp,
            comparingLong(Instant::toEpochMilli)
    );

    private static final Comparator<MessageWithTimestamp> hundredMillisGranularityComparator = comparing(
            MessageWithTimestamp::getTimestamp,
            comparingLong(i -> {
                long millis = i.toEpochMilli();
                return (millis / 100) * 100; // comparison granularity is 100 millis
            })
    );

    private static final Comparator<MessageWithTimestamp> oneSecondGranularityComparator = comparing(
            MessageWithTimestamp::getTimestamp,
            comparingLong(Instant::getEpochSecond)
    );


    private static final Comparator<MessageWithTimestamp> oneMinuteGranularityComparator = comparing(
            MessageWithTimestamp::getTimestamp,
            comparingLong(i -> i.getEpochSecond() / 60)
    );


    private static final Comparator<MessageWithTimestamp> oneHourGranularityComparator = comparing(
            MessageWithTimestamp::getTimestamp,
            comparingLong(i -> i.getEpochSecond() / (60 * 60))
    );


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

    public static List<Comparator<MessageWithTimestamp>> comparatorsOrderedByGranularity() {
        return asList(
                oneMilliGranularityComparator,
                hundredMillisGranularityComparator,
                oneSecondGranularityComparator,
                oneMinuteGranularityComparator,
                oneHourGranularityComparator
        );
    }

    @Override
    public String toString() {
        return "MessageWithTimestamp{" +
                "message=" + message +
                ", timestamp=" + String.valueOf((timestamp.getEpochSecond())) + " sec " + String.valueOf((timestamp.toEpochMilli() % 1000)) + " millis" +
                '}';
    }
}
