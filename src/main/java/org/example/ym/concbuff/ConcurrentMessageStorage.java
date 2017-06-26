/* Copyright 2017 Sabre Holdings */
package org.example.ym.concbuff;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.binarySearch;
import static java.util.stream.Collectors.toList;

public class ConcurrentMessageStorage implements MessageStorage {
    private static final int BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS = 5;

    private static final int INITIAL_BUCKET_ARR_SIZE = 5_000;
    private static final int BINARY_SEARCH_MIN_THRESHOLD = 50;

    private final Duration keepAliveDuration;
    private final Duration keepAliveDurationForBuckets;
    private final ConcurrentHashMap<Instant, Bucket> timedBuckets = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Instant> sortedAvailableInstants = new ConcurrentLinkedDeque<>();


    private ConcurrentMessageStorage(int messagesKeepTime, TimeUnit unit) {
        this.keepAliveDuration = Duration.ofNanos(unit.toNanos(messagesKeepTime));
        this.keepAliveDurationForBuckets = this.keepAliveDuration.plusMillis(BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100);
    }


    public static ConcurrentMessageStorage newConcurrentMessageStorage(int messagesKeepTime, TimeUnit unit) {
        checkArgument(unit != TimeUnit.NANOSECONDS && unit != TimeUnit.MICROSECONDS, "To small time unit " + unit.name());
        checkArgument(unit != TimeUnit.DAYS, "To large time unit");

        return new ConcurrentMessageStorage(messagesKeepTime, unit);
    }

    @Override
    public void storeMessage(Message message) {
        Instant now = Instant.now();

        //todo maybe merge this methods together
        Instant nowNormalized = normalizeToBucketGranularityFlor(now);

        final Bucket targetBucket = timedBuckets.computeIfAbsent(nowNormalized, tst -> new Bucket());
        synchronized (targetBucket) {

            if (!targetBucket.isMarked()) {
                sortedAvailableInstants.addLast(nowNormalized);
                targetBucket.mark();
            }

            targetBucket.addMessage(new MessageWithTimestamp(message, now));

        }

        cleanUpForNow(now);

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
        final Instant now = Instant.now();

        if (quantity <= 0) {
            return Stream.empty();
        }

        // --------------
        Instant lastAcceptableBucketTimestamp = lastAcceptableBucketNormalizedTimestampForNow(now);
        BucketNormalizedTimestamps timestampsIter = new BucketNormalizedTimestamps(now, lastAcceptableBucketTimestamp);

        LinkedList<List<MessageWithTimestamp>> gatheredChunksWithinKeepAlive = new LinkedList<>();
        int chunksAccumulatedSize = 0;

        while (chunksAccumulatedSize < quantity && timestampsIter.hasNext()){

            Instant currBucketTimestamp = timestampsIter.next();
            final Bucket currentBucket = timedBuckets.get(currBucketTimestamp);

            if (currentBucket != null) {
                synchronized (currentBucket) {
                     if (!currentBucket.isEmpty()) {
                        gatheredChunksWithinKeepAlive.addFirst(new ArrayList<>(currentBucket.getMessages()));
                        chunksAccumulatedSize += currentBucket.messageCount();
                    }
                }
            }
            // todo should synchronize and copy only in 'Danger zone' (1-2 buckets close to current inserting point)

        }

        if (chunksAccumulatedSize == 0 || gatheredChunksWithinKeepAlive.isEmpty()) {
            return Stream.empty();
        }

        List<MessageWithTimestamp> lastChunk = gatheredChunksWithinKeepAlive.pollFirst();
        int firstAcceptableMessageIndexInLastChunk = findFirstAcceptableMessageIndex(lastAcceptableTimestampForNow(now), lastChunk);
        if (firstAcceptableMessageIndexInLastChunk >= 0 && firstAcceptableMessageIndexInLastChunk < lastChunk.size()) {
            gatheredChunksWithinKeepAlive.addFirst(lastChunk.subList(firstAcceptableMessageIndexInLastChunk, lastChunk.size()));
        }

        LinkedList<List<MessageWithTimestamp>> gatheredChunksWithQuantity = new LinkedList<>();
        int quantityToGatherLeft = quantity;
        while (quantityToGatherLeft > 0 && !gatheredChunksWithinKeepAlive.isEmpty()) {
            List<MessageWithTimestamp> latest = gatheredChunksWithinKeepAlive.pollLast();

            if (latest.size() < quantityToGatherLeft) {
                gatheredChunksWithQuantity.addFirst(latest);
                quantityToGatherLeft -= latest.size();
            } else {
                gatheredChunksWithQuantity.addFirst(latest.subList(latest.size() - quantityToGatherLeft, latest.size()));
                quantityToGatherLeft = 0;
            }
        }

        cleanUpForNow(now);

        return gatheredChunksWithQuantity.stream()
                .flatMap(List::stream)
                .map(MessageWithTimestamp::getMessage);
    }

    private static class BucketNormalizedTimestamps implements Iterator<Instant> {
        private Instant currentBucketTimeStamp;
        private final Instant lastAcceptableBucketTimestamp;

        public BucketNormalizedTimestamps(Instant now, Instant lastAcceptableBucketTimestamp) {
            this.lastAcceptableBucketTimestamp = lastAcceptableBucketTimestamp;
            this.currentBucketTimeStamp = normalizeToBucketGranularityFlor(now).plusMillis(BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100);
        }

        @Override
        public boolean hasNext() {
            return currentBucketTimeStamp.isAfter(lastAcceptableBucketTimestamp);
        }

        @Override
        public Instant next() {
            currentBucketTimeStamp = currentBucketTimeStamp.minusMillis(BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100);
            return currentBucketTimeStamp;
        }
    }

    private void cleanUpForNow(Instant now) {
        Instant latestBucketNormalizedTimestamp = sortedAvailableInstants.pollFirst();
        if (latestBucketNormalizedTimestamp != null) {
            if (latestBucketNormalizedTimestamp.isBefore(lastAcceptableBucketNormalizedTimestampForNow(now))) {
                timedBuckets.remove(latestBucketNormalizedTimestamp);
            }
            else {
                sortedAvailableInstants.addFirst(latestBucketNormalizedTimestamp);
            }
        }
    }


    private Instant lastAcceptableTimestampForNow(Instant now) {
        return now.minus(keepAliveDuration);
    }

    private Instant lastAcceptableBucketNormalizedTimestampForNow(Instant now) {
        return normalizeToBucketGranularityFlor(now.minus(keepAliveDuration));
    }

    private static Instant normalizeToBucketGranularityFlor(Instant instant) {

        long millis = instant.toEpochMilli();
        int bucketGranularity = BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100;
        long normalizedMillis = (millis / bucketGranularity) * bucketGranularity;

        return Instant.ofEpochMilli(normalizedMillis);
    }

    private static Instant normalizeToBucketGranularityCeiling(Instant instant) {

        long millis = instant.toEpochMilli();
        int bucketGranularity = BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100;
        long normalizedMillis = (1 + millis / bucketGranularity) * bucketGranularity;

        return Instant.ofEpochMilli(normalizedMillis);
    }

    private int findFirstAcceptableMessageIndex(Instant lastAcceptable, List<MessageWithTimestamp> messages) {
        int idx = Math.max(0, tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(lastAcceptable, messages));
        return findFirstAcceptableMessageIdxStartingFrom(idx, lastAcceptable, messages);
    }

    private int tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(Instant lastAcceptable, List<MessageWithTimestamp> messages) {
        if (messages.size() < BINARY_SEARCH_MIN_THRESHOLD) {
            return -1;
        }

        MessageWithTimestamp searchTarget = MessageWithTimestamp.binarySearchTimestamp(lastAcceptable);
        List<Comparator<MessageWithTimestamp>> comparators = MessageWithTimestamp.comparatorsOrderedByGranularity();

        int idx = -1;
        for (Iterator<Comparator<MessageWithTimestamp>> itComparator = comparators.iterator(); idx < 0 && itComparator.hasNext(); ) {
            idx = binarySearch(messages, searchTarget, itComparator.next());
        }

        return idx;
    }

    private int findFirstAcceptableMessageIdxStartingFrom(int idx, Instant lastAcceptable, List<MessageWithTimestamp> messages) {
        while (idx > 0 && messages.get(idx).timestamp.isAfter(lastAcceptable)) {
            idx--;
        }

        while (idx < messages.size() && messages.get(idx).timestamp.isBefore(lastAcceptable)) {
            idx++;
        }
        return idx;
    }


    private static class Bucket {

        // TODO: 26.06.17 to be removed
        private boolean isMarked = false;

        private ArrayList<MessageWithTimestamp> messages = new ArrayList<>(INITIAL_BUCKET_ARR_SIZE);

        // TODO: 26.06.17 to be removed
        public void initializeFor(Instant normalizedTimestamp) {
            messages = new ArrayList<>(INITIAL_BUCKET_ARR_SIZE);
            isMarked = false;
        }

        public ArrayList<MessageWithTimestamp> getMessages() {
            return messages;
        }

        public boolean isEmpty() {
            return messages.isEmpty();
        }

        public int messageCount() {
            return messages.size();
        }

        public void addMessage(MessageWithTimestamp messageWithTimestamp) {
            isMarked = true;
            messages.add(messageWithTimestamp);
        }

        public boolean isMarked() {
            return isMarked;
        }

        public void mark() {
            isMarked = true;
        }

        @Override
        public String toString() {
            return "Bucket{" +
                    "" + messages +
                    '}';
        }
    }

    public static void main(String[] args) {
        List<Integer> a = asList(1, 2, 3);
        List<Integer> b = asList(4, 5, 6);
        List<Integer> c = asList(7, 8, 9);


        System.out.println(asList(a, b, c).stream()
                .flatMap(List::stream)
                .collect(toList()));

    }
}
