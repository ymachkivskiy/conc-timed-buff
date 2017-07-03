package org.example.ym.concbuff;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.time.Duration.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public enum MessageWithTimestampComparator implements Comparator<MessageWithTimestamp> {

    TEN_MICROSECONDS_GRANULARITY(
            ofNanos(10 * 1_000),
            Comparator.comparing(MessageWithTimestamp::getTimestamp,
                    Comparator.comparingLong(i -> i.getEpochSecond() * 1_000 * 1_000 + (i.getNano() / (1_000 * 10)) * 10))
    ),

    ONE_HUNDRED_MICROSECONDS_GRANULARITY(
            ofNanos(100 * 1_000),
            Comparator.comparing(MessageWithTimestamp::getTimestamp,
                    Comparator.comparingLong(i -> i.getEpochSecond() * 1_000 * 1_000 + (i.getNano() / (1_000 * 100)) * 100))
    ),

    ONE_MILLISECOND_GRANULARITY(
            ofMillis(1),
            Comparator.comparing(MessageWithTimestamp::getTimestamp, Comparator.comparingLong(Instant::toEpochMilli))
    ),

    ONE_HUNDRED_MILLISECONDS_GRANULARITY(
            ofMillis(100),
            Comparator.comparing(MessageWithTimestamp::getTimestamp,
                    Comparator.comparingLong(i -> {
                        long millis = i.toEpochMilli();
                        return (millis / 100) * 100;
                    })
    )),

    ONE_SECOND_GRANULARITY(
            ofSeconds(1),
            Comparator.comparing(MessageWithTimestamp::getTimestamp, Comparator.comparingLong(Instant::getEpochSecond))
    ),

    ONE_MINUTE_GRANULARITY(
            ofMinutes(1),
            Comparator.comparing(MessageWithTimestamp::getTimestamp, Comparator.comparingLong(i -> i.getEpochSecond() / 60))
    ),

    ONE_HOUR_GRANULARITY(
            ofHours(1),
            Comparator.comparing(MessageWithTimestamp::getTimestamp, Comparator.comparingLong(i -> i.getEpochSecond() / (60 * 60)))
    );

    private final Duration granularity;
    private final Comparator<MessageWithTimestamp> cmp;

    MessageWithTimestampComparator(Duration granularity, Comparator<MessageWithTimestamp> cmp) {
        this.granularity = granularity;
        this.cmp = cmp;
    }

    public static List<Comparator<MessageWithTimestamp>> comparatorsForGranularity(Duration desiredGranularity) {
        return Arrays.stream(values())
                .filter(comparatorCandidate -> comparatorCandidate.granularity.compareTo(desiredGranularity) < 0)
                .collect(toList());
    }

    public static List<Comparator<MessageWithTimestamp>> comparators() {
        return asList(values());
    }

    @Override
    public int compare(MessageWithTimestamp messageWithTimestamp, MessageWithTimestamp t1) {
        return cmp.compare(messageWithTimestamp, t1);
    }

}
