package org.example.ym.concbuff;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.binarySearch;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;

public class NaiveMessageStorage implements MessageStorage {

    private static final int INITIAL_BUFF_SIZE = 10_000;
    private static final int CLEAN_UP_THRESHOLD_SIZE = 100_000;

    private final Duration keepAliveDuration;
    private List<MessageWithTimestamp> messages = new ArrayList<>(INITIAL_BUFF_SIZE);

    private NaiveMessageStorage(int messagesKeepTime, TimeUnit unit) {
        keepAliveDuration = Duration.ofNanos(unit.toNanos(messagesKeepTime));
    }

    public static NaiveMessageStorage newNaiveMessageStorage(int messagesKeepTime, TimeUnit unit) {
        checkArgument(unit != TimeUnit.NANOSECONDS && unit != TimeUnit.MICROSECONDS, "To small time unit " + unit.name());
        checkArgument(unit != TimeUnit.DAYS, "To large time unit");

        return new NaiveMessageStorage(messagesKeepTime, unit);
    }

    @Override
    public void storeMessage(Message message) {
        Instant now = Instant.now();

        MessageWithTimestamp messageWithTimestamp = new MessageWithTimestamp(message, now);

        synchronized (this) {

            if (messages.size() >= CLEAN_UP_THRESHOLD_SIZE) {
                tryCleanUpSpaceForCurrentMoment(now);
            }

            messages.add(messageWithTimestamp);
        }
    }

    @Override
    public List<Message> queryLatest(int quantity) {
        return internalQueryLatestStream(quantity).collect(toList());
    }

    @Override
    public int countLatestMatching(int quantity, Predicate<Message> predicate) {
        return (int) internalQueryLatestStream(quantity)
                .filter(predicate)
                .count();
    }

    private Stream<Message> internalQueryLatestStream(int quantity) {
        Instant now = Instant.now();

        if (quantity <= 0) {
            return Stream.empty();
        }

        List<MessageWithTimestamp> result;

        synchronized (this) {

            tryCleanUpSpaceForCurrentMoment(now);

            if (messages.isEmpty()) {
                return Stream.empty();
            }

            result = internalSnapshotOfLatestMessages(quantity);

        }

        return result.stream().map(MessageWithTimestamp::getMessage);

    }

    private void tryCleanUpSpaceForCurrentMoment(Instant now) {
        if (messages.isEmpty()) {
            return;
        }

        Instant lastAcceptableTimestamp = now.minus(keepAliveDuration);

        if (lastMessageExceededKeepAliveTime(lastAcceptableTimestamp)) {
            messages = new ArrayList<>(INITIAL_BUFF_SIZE);
        } else if (firstMessageExceededKeepAliveTime(lastAcceptableTimestamp)) {
            dropMessagesWhichExceededKeepAliveTime(lastAcceptableTimestamp);
        }
    }

    private boolean lastMessageExceededKeepAliveTime(Instant lastAcceptableTimestamp) {
        return messages.get(messages.size() - 1).timestamp.isBefore(lastAcceptableTimestamp);
    }

    private boolean firstMessageExceededKeepAliveTime(Instant lastAcceptableTimestamp) {
        return messages.get(0).timestamp.isBefore(lastAcceptableTimestamp);
    }

    private void dropMessagesWhichExceededKeepAliveTime(Instant lastAcceptableTimestamp) {

        int firstIdxOfKeepAliveIdx = findFirstAcceptableMessageIndex(lastAcceptableTimestamp);

        if (firstIdxOfKeepAliveIdx >= 0 && firstIdxOfKeepAliveIdx < messages.size()) {
            List<MessageWithTimestamp> messagesWithinKeepAliveTime = messages.subList(firstIdxOfKeepAliveIdx, messages.size());
            messages = new ArrayList<>(Math.max(messagesWithinKeepAliveTime.size() * 2, INITIAL_BUFF_SIZE));
            messages.addAll(messagesWithinKeepAliveTime);
        } else {
            messages = new ArrayList<>(INITIAL_BUFF_SIZE);
        }

    }

    private int findFirstAcceptableMessageIndex(Instant lastAcceptable) {
        int idx = tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(lastAcceptable);
        return findFirstAcceptableMessageIdxStartingFrom(idx, lastAcceptable);
    }

    private int tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(Instant lastAcceptable) {
        MessageWithTimestamp searchTarget = MessageWithTimestamp.binarySearchTimestamp(lastAcceptable);
        List<Comparator<MessageWithTimestamp>> comparators = MessageWithTimestamp.comparatorsOrderedByGranularity();

        int idx = -1;
        for (Iterator<Comparator<MessageWithTimestamp>> itComparator = comparators.iterator(); idx < 0 && itComparator.hasNext(); ) {
            idx = binarySearch(messages, searchTarget, itComparator.next());
        }

        return Math.max(0, idx);
    }

    private int findFirstAcceptableMessageIdxStartingFrom(int idx, Instant lastAcceptable) {
        while (idx < messages.size() && messages.get(idx).timestamp.isBefore(lastAcceptable)) {
            idx++;
        }
        return idx;
    }

    private ArrayList<MessageWithTimestamp> internalSnapshotOfLatestMessages(int quantity) {
        int startIdx = Math.max(0, messages.size() - quantity);
        return new ArrayList<>(messages.subList(startIdx, messages.size()));
    }

    private static class MessageWithTimestamp {

        private static final Comparator<MessageWithTimestamp> hundredMillisGranularityComparator = comparing(
                MessageWithTimestamp::getTimestamp,
                comparingLong(Instant::getEpochSecond)
                        .thenComparingLong(i -> {
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
        private final Instant timestamp;

        private MessageWithTimestamp(Message message, Instant timestamp) {
            this.message = message;
            this.timestamp = timestamp;
        }

        private static MessageWithTimestamp binarySearchTimestamp(Instant timestamp) {
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
                    hundredMillisGranularityComparator,
                    oneSecondGranularityComparator,
                    oneMinuteGranularityComparator,
                    oneHourGranularityComparator
            );
        }
    }
}
