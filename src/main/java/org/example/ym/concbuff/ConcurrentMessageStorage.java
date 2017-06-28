/* Copyright 2017 Sabre Holdings */
package org.example.ym.concbuff;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.binarySearch;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.example.ym.concbuff.MessageWithTimestamp.binarySearchTimestamp;
import static org.example.ym.concbuff.MessageWithTimestamp.comparatorsOrderedByGranularity;

public class ConcurrentMessageStorage implements MessageStorage {

    private static final int BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS = 5;
    private static final int INITIAL_BUCKET_ARR_SIZE = 5_000;
    private static final int CLEAN_UP_FREQUENCY = 50;

    private static final int BINARY_SEARCH_MIN_THRESHOLD = 50;

    private final ConcurrentHashMap<Instant, Bucket> storageBuckets = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Instant> bucketIdentifiersForCleanUp = new ConcurrentLinkedDeque<>();

    private final Duration keepAliveDuration;

    private volatile int estimatedWritesCount;


    private ConcurrentMessageStorage(int messagesKeepTime, TimeUnit unit) {
        this.keepAliveDuration = Duration.ofNanos(unit.toNanos(messagesKeepTime));
    }

    public static ConcurrentMessageStorage newConcurrentMessageStorage(int messagesKeepTime, TimeUnit unit) {
        checkArgument(unit != TimeUnit.NANOSECONDS && unit != TimeUnit.MICROSECONDS, "To small time unit " + unit.name());
        checkArgument(unit != TimeUnit.DAYS, "To large time unit");

        return new ConcurrentMessageStorage(messagesKeepTime, unit);
    }

    @Override
    public void storeMessage(Message message) {

        final Instant now = Instant.now();

        Bucket targetBucket = getStorageBucketForCurrentInstant(now);
        storeMessageInBucket(targetBucket, new MessageWithTimestamp(message, now));

        tryCleanUp(now);

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

    private Bucket getStorageBucketForCurrentInstant(Instant now) {
        return storageBuckets.computeIfAbsent(normalizeToBucketGranularity(now), Bucket::new);
    }

    private void storeMessageInBucket(Bucket targetBucket, MessageWithTimestamp messageWithTimestamp) {
        try{
            targetBucket.lock.writeLock().lock();

            targetBucket.addMessage(messageWithTimestamp);

            rememberBucketIdentifierForCleanUpPurposes(targetBucket);

        }finally {
            targetBucket.lock.writeLock().unlock();
        }
    }

    private void rememberBucketIdentifierForCleanUpPurposes(Bucket targetBucket) {
        if (!targetBucket.isRememberedForCleanUp()) {
            bucketIdentifiersForCleanUp.addLast(targetBucket.identifier);
            targetBucket.markAsRemembered();
        }
    }

    private void tryCleanUp(Instant now) {
        // increment does not need to be atomic for estimation
        if (Math.floorMod(++estimatedWritesCount, CLEAN_UP_FREQUENCY) == 0)
        {
            Instant oldestBucketIdentifier = bucketIdentifiersForCleanUp.pollFirst();
            if (oldestBucketIdentifier != null) {
                if (oldestBucketIdentifier.isBefore(oldestAcceptableBucketIdentifierFor(now))) {
                    storageBuckets.remove(oldestBucketIdentifier);
                }
                else {
                    bucketIdentifiersForCleanUp.addFirst(oldestBucketIdentifier);
                }
            }
        }
    }

    private Stream<Message> internalQueryLatestStream(int quantity) {

        final Instant now = Instant.now();

        if (quantity <= 0) {
            return Stream.empty();
        }

        LinkedList<List<MessageWithTimestamp>> chunksWithinKeepAlive = gatherOrderedChunksWithinKeepAliveForQuantity(now, quantity);

        if (chunksWithinKeepAlive.isEmpty()) {
            return Stream.empty();
        }

        dropExceededKeepAliveInOldestChunk(chunksWithinKeepAlive, now);

        return convertToMessagesStream(getChunksWithDesiredQuantity(chunksWithinKeepAlive, quantity));
    }

    private LinkedList<List<MessageWithTimestamp>> gatherOrderedChunksWithinKeepAliveForQuantity(Instant now, int quantity) {

        LinkedList<List<MessageWithTimestamp>> orderedChunks = new LinkedList<>();
        int chunksAccumulatedSize = 0;

        for (WithinKeepAliveBucketsIterator it = new WithinKeepAliveBucketsIterator(now);
             chunksAccumulatedSize < quantity && it.hasNext();){

            List<MessageWithTimestamp> messages = it.next().readMessages();

            if (!messages.isEmpty()) {
                orderedChunks.addFirst(messages);
                chunksAccumulatedSize += messages.size();
            }

        }

        return orderedChunks;
    }

    private void dropExceededKeepAliveInOldestChunk(LinkedList<List<MessageWithTimestamp>> chunksWithinKeepAlive, Instant now) {
        List<MessageWithTimestamp> oldestChunk = chunksWithinKeepAlive.pollFirst();
        List<MessageWithTimestamp> oldestChunkWithOnlyKeepAliveMessages = dropMessagesExceedingKeepAlive(oldestChunk, lastAcceptableTimestampFor(now));
        chunksWithinKeepAlive.addFirst(oldestChunkWithOnlyKeepAliveMessages);
    }

    private static LinkedList<List<MessageWithTimestamp>> getChunksWithDesiredQuantity(LinkedList<List<MessageWithTimestamp>> chunks, int desiredQuantity) {
        LinkedList<List<MessageWithTimestamp>> result = new LinkedList<>();

        while (desiredQuantity > 0 && !chunks.isEmpty()) {
            List<MessageWithTimestamp> currentChunk = chunks.pollLast();

            if (currentChunk.size() < desiredQuantity) {
                result.addFirst(currentChunk);
                desiredQuantity -= currentChunk.size();
            } else {
                result.addFirst(currentChunk.subList(currentChunk.size() - desiredQuantity, currentChunk.size()));
                desiredQuantity = 0;
            }
        }

        return result;
    }


    private Instant lastAcceptableTimestampFor(Instant now) {
        return now.minus(keepAliveDuration);
    }

    private Instant oldestAcceptableBucketIdentifierFor(Instant now) {
        return normalizeToBucketGranularity(now.minus(keepAliveDuration));
    }

    //region Util functions

    private static Instant normalizeToBucketGranularity(Instant instant) {

        long millis = instant.toEpochMilli();
        int bucketGranularity = BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100;
        long normalizedMillis = (millis / bucketGranularity) * bucketGranularity;

        return Instant.ofEpochMilli(normalizedMillis);
    }

    private static List<MessageWithTimestamp> dropMessagesExceedingKeepAlive(List<MessageWithTimestamp> messages, Instant oldestAcceptableTimestamp) {

        int idx = tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(messages, oldestAcceptableTimestamp);
        idx = findFirstAcceptableMessageIdxSequentially(messages, idx, oldestAcceptableTimestamp);

        if (idx >= 0 && idx < messages.size()) {
            return messages.subList(idx, messages.size());
        }

        return emptyList();
    }

    private  static int tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(List<MessageWithTimestamp> messages, Instant lastAcceptable) {
        if (messages.size() < BINARY_SEARCH_MIN_THRESHOLD) {
            return 0;
        }

        int estimatedIdx = comparatorsOrderedByGranularity().stream()
                .mapToInt(comparator -> binarySearch(messages, binarySearchTimestamp(lastAcceptable), comparator))
                .filter(idx -> idx >= 0)
                .findFirst()
                .orElse(0);

        // correct binary search errors
        while (estimatedIdx > 0 && messages.get(estimatedIdx).timestamp.isAfter(lastAcceptable)) {
            estimatedIdx--;
        }

        return estimatedIdx;
    }

    private static int findFirstAcceptableMessageIdxSequentially(List<MessageWithTimestamp> messages, int startingIdx, Instant lastAcceptable) {

        while (startingIdx < messages.size() && messages.get(startingIdx).timestamp.isBefore(lastAcceptable)) {
            startingIdx++;
        }

        return startingIdx;
    }

    private static Stream<Message> convertToMessagesStream(LinkedList<List<MessageWithTimestamp>> chunks) {
        return chunks.stream()
                .flatMap(List::stream)
                .map(MessageWithTimestamp::getMessage);
    }

    //endregion

    private class WithinKeepAliveBucketsIterator implements Iterator<BucketForReader> {

        private final Instant oldestAcceptableBucketIdentifier;
        private Instant currentBucketIdentifier;

        public WithinKeepAliveBucketsIterator(Instant now) {
            this.oldestAcceptableBucketIdentifier = oldestAcceptableBucketIdentifierFor(now);
            this.currentBucketIdentifier = normalizeToBucketGranularity(now).plusMillis(BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100);
        }

        @Override
        public boolean hasNext() {
            return currentBucketIdentifier.isAfter(oldestAcceptableBucketIdentifier);
        }

        @Override
        public BucketForReader next() {
            currentBucketIdentifier = currentBucketIdentifier.minusMillis(BUCKET_TIME_GRANULARITY_IN_HUNDREDS_MILLIS * 100);
            Bucket bucket = storageBuckets.get(currentBucketIdentifier);
            return new BucketForReader(bucket);
        }

    }

    private static class Bucket {
        public final ReadWriteLock lock = new ReentrantReadWriteLock();

        private final Instant identifier;
        private boolean isRememberedForCleanUp = false;

        private ArrayList<MessageWithTimestamp> messages = new ArrayList<>(INITIAL_BUCKET_ARR_SIZE);

        public Bucket(Instant identifier) {
            this.identifier = identifier;
        }

        public List<MessageWithTimestamp> getMessages() {
            return unmodifiableList(messages);
        }

        public void addMessage(MessageWithTimestamp messageWithTimestamp) {
            messages.add(messageWithTimestamp);
        }

        public boolean isRememberedForCleanUp() {
            return isRememberedForCleanUp;
        }

        public void markAsRemembered() {
            isRememberedForCleanUp = true;
        }

        @Override
        public String toString() {
            return "Bucket{" +
                    "" + messages +
                    '}';
        }
    }

    private static class BucketForReader {
        private final Optional<Bucket> bucket;

        private BucketForReader(Bucket bucket) {
            this.bucket = ofNullable(bucket);
        }

        public List<MessageWithTimestamp> readMessages() {
            return bucket
                    .map(this::readFromBucket)
                    .orElseGet(Collections::emptyList);
        }

        private List<MessageWithTimestamp> readFromBucket(Bucket b) {
            // todo: should synchronize and copy only in 'Danger zone' (1-2 buckets close to current inserting point), other are not filled any more

            try{
                b.lock.readLock().lock();

                if (!b.getMessages().isEmpty()) {
                    return new ArrayList<>(b.getMessages());
                }

                return emptyList();

            }finally {
                b.lock.readLock().unlock();
            }

        }

    }

}
