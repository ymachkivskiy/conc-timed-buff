/* Copyright 2017 Sabre Holdings */
package org.example.ym.concbuff;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.time.Duration.ofMillis;
import static java.util.Collections.binarySearch;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.concat;
import static org.example.ym.concbuff.TimestampComparator.comparatorsForGranularity;

public class ConcurrentMessageStorage implements MessageStorage {

    private static final int BUCKET_TIME_GRANULARITY_IN_MILLIS = 1;
    private static final int INITIAL_BUCKET_ARR_SIZE = 25;
    private static final int CLEAN_UP_FREQUENCY = 100;

    private static final int BINARY_SEARCH_MIN_THRESHOLD = 8;

    private static final List<Comparator<Instant>> BINARY_SEARCH_COMPARATORS = comparatorsForGranularity(ofMillis(BUCKET_TIME_GRANULARITY_IN_MILLIS));

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
        storeMessageInBucket(targetBucket, message, now);

        tryCleanUp(now);

    }

    @Override
    public List<Message> queryLatest(int quantity) {
        List<List<Message>> chunks = internalQueryLatestChunks(quantity);

        List<Message> result = new ArrayList<>(summaryMessageQuantity(chunks));

        for (List<Message> chunk : chunks) {
            result.addAll(chunk);
        }

        return result;
    }

    @Override
    public int countLatestMatching(int quantity, Predicate<Message> predicate) {
        return (int) internalQueryLatestChunks(quantity).stream()
                .flatMap(List::stream)
                .filter(predicate)
                .count();
    }

    private Bucket getStorageBucketForCurrentInstant(Instant now) {
        return storageBuckets.computeIfAbsent(normalizeToBucketGranularity(now), Bucket::new);
    }

    private void storeMessageInBucket(Bucket targetBucket, Message message, Instant timestamp) {

        boolean shouldRememberForCleanUp = writeToBucketWithLock(targetBucket, message, timestamp);

        if (shouldRememberForCleanUp) {
            bucketIdentifiersForCleanUp.addLast(targetBucket.identifier);
        }

    }

    private boolean writeToBucketWithLock(Bucket targetBucket, Message message, Instant timestamp) {
        long stamp = targetBucket.lock.writeLock();
        try {

            targetBucket.addMessage(message, timestamp);

            if (!targetBucket.isRememberedForCleanUp()) {
                targetBucket.markAsRemembered();
                return true;
            }

            return false;

        } finally {
            targetBucket.lock.unlockWrite(stamp);
        }
    }

    private void tryCleanUp(Instant now) {
        // increment does not need to be atomic for estimation
        if (Math.floorMod(++estimatedWritesCount, CLEAN_UP_FREQUENCY) == 0) {
            Instant oldestBucketIdentifier = bucketIdentifiersForCleanUp.pollFirst();
            if (oldestBucketIdentifier != null) {
                if (oldestBucketIdentifier.isBefore(oldestAcceptableBucketIdentifierFor(now))) {
                    storageBuckets.remove(oldestBucketIdentifier);
                } else {
                    bucketIdentifiersForCleanUp.addFirst(oldestBucketIdentifier);
                }
            }
        }
    }

    private List<List<Message>> internalQueryLatestChunks(int quantity) {

        final Instant now = Instant.now();

        if (quantity <= 0) {
            return emptyList();
        }

        LinkedList<MessagesWithTimestamps> chunksWithinKeepAlive = gatherOrderedChunksWithinKeepAliveForQuantity(now, quantity);

        if (chunksWithinKeepAlive.isEmpty()) {
            return emptyList();
        }

        LinkedList<List<Message>> messagesWithinKeepAlive = dropExceededKeepAliveInOldestChunk(chunksWithinKeepAlive, now);

        if (summaryMessageQuantity(messagesWithinKeepAlive) <= quantity) {
            return messagesWithinKeepAlive;
        } else {
            return getChunksWithDesiredQuantity(messagesWithinKeepAlive, quantity);
        }
    }

    private LinkedList<MessagesWithTimestamps> gatherOrderedChunksWithinKeepAliveForQuantity(Instant now, int quantity) {

        LinkedList<MessagesWithTimestamps> orderedChunks = new LinkedList<>();
        int chunksAccumulatedSize = 0;

        for (WithinKeepAliveBucketsIterator it = new WithinKeepAliveBucketsIterator(now);
             chunksAccumulatedSize < quantity && it.hasNext(); ) {

            MessagesWithTimestamps messages = it.next().readMessages();

            if (!messages.isEmpty()) {
                orderedChunks.addFirst(messages);
                chunksAccumulatedSize += messages.size();
            }

        }

        return orderedChunks;
    }

    private LinkedList<List<Message>> dropExceededKeepAliveInOldestChunk(LinkedList<MessagesWithTimestamps> chunksWithinKeepAlive, Instant now) {

        if (chunksWithinKeepAlive.getFirst().mayContainMessagesWithExceededKeepAlive()) {

            List<Message> oldestChunkWithOnlyKeepAliveMessages = getMessagesOnlyInKeepAlive(chunksWithinKeepAlive.pollFirst(), lastAcceptableTimestampFor(now));

            return concat(
                    Stream.of(oldestChunkWithOnlyKeepAliveMessages),
                    chunksWithinKeepAlive.stream().map(MessagesWithTimestamps::getMessages))
                    .collect(toCollection(LinkedList::new));
        } else {
            return chunksWithinKeepAlive.stream()
                    .map(MessagesWithTimestamps::getMessages)
                    .collect(toCollection(LinkedList::new));
        }

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
        int bucketGranularity = BUCKET_TIME_GRANULARITY_IN_MILLIS;
        long normalizedMillis = (millis / bucketGranularity) * bucketGranularity;

        return Instant.ofEpochMilli(normalizedMillis);
    }

    private static LinkedList<List<Message>> getChunksWithDesiredQuantity(LinkedList<List<Message>> chunks, int desiredQuantity) {
        final int chunksSummaryMessagesNumber = summaryMessageQuantity(chunks);

        List<Message> oldestChunk = chunks.pollFirst();

        int summarySizeOfAllExceptOldest = chunksSummaryMessagesNumber - oldestChunk.size();
        int messagesToTakeFromOldest = desiredQuantity - summarySizeOfAllExceptOldest;

        if (messagesToTakeFromOldest > 0) {
            chunks.addFirst(oldestChunk.subList(oldestChunk.size() - messagesToTakeFromOldest, oldestChunk.size()));
        }

        return chunks;
    }

    private static List<Message> getMessagesOnlyInKeepAlive(MessagesWithTimestamps messages, Instant oldestAcceptableTimestamp) {

        int idx = tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(messages.getTimestamps(), oldestAcceptableTimestamp);
        idx = findFirstAcceptableMessageIdxSequentially(messages.getTimestamps(), idx, oldestAcceptableTimestamp);

        if (idx >= 0 && idx < messages.size()) {
            return messages.getMessages().subList(idx, messages.size());
        }

        return emptyList();

    }

    private static int tryFindNearestIdxToFirstAcceptableMessageUsingBinarySearch(List<Instant> timestamps, Instant lastAcceptable) {
        if (timestamps.size() < BINARY_SEARCH_MIN_THRESHOLD) {
            return 0;
        }

        int estimatedIdx = BINARY_SEARCH_COMPARATORS.stream()
                .mapToInt(comparator -> binarySearch(timestamps, lastAcceptable, comparator))
                .filter(idx -> idx >= 0)
                .findFirst()
                .orElse(0);

        // correct binary search errors
        while (estimatedIdx > 0 && timestamps.get(estimatedIdx).isAfter(lastAcceptable)) {
            estimatedIdx--;
        }

        return estimatedIdx;
    }

    private static int findFirstAcceptableMessageIdxSequentially(List<Instant> timestamps, int startingIdx, Instant lastAcceptable) {

        while (startingIdx < timestamps.size() && timestamps.get(startingIdx).isBefore(lastAcceptable)) {
            startingIdx++;
        }

        return startingIdx;
    }

    private static int summaryMessageQuantity(List<List<Message>> chunks) {
        return chunks.stream()
                .mapToInt(List::size)
                .sum();
    }

    //endregion

    private final class WithinKeepAliveBucketsIterator implements Iterator<BucketForReader> {

        private final Instant oldestAcceptableBucketIdentifier;
        private Instant currentBucketIdentifier;

        public WithinKeepAliveBucketsIterator(Instant now) {
            this.oldestAcceptableBucketIdentifier = oldestAcceptableBucketIdentifierFor(now);
            this.currentBucketIdentifier = normalizeToBucketGranularity(now).plusMillis(BUCKET_TIME_GRANULARITY_IN_MILLIS);
        }

        @Override
        public boolean hasNext() {
            return currentBucketIdentifier.isAfter(oldestAcceptableBucketIdentifier);
        }

        @Override
        public BucketForReader next() {
            currentBucketIdentifier = currentBucketIdentifier.minusMillis(BUCKET_TIME_GRANULARITY_IN_MILLIS);
            Bucket bucket = storageBuckets.get(currentBucketIdentifier);
            return new BucketForReader(bucket, !hasNext());
        }

    }

    private static final class Bucket {
        public final StampedLock lock = new StampedLock();

        private final Instant identifier;
        private boolean isRememberedForCleanUp = false;

        private final ArrayList<Message> messages = new ArrayList<>(INITIAL_BUCKET_ARR_SIZE);
        private final ArrayList<Instant> timestamps = new ArrayList<>(INITIAL_BUCKET_ARR_SIZE);

        public Bucket(Instant identifier) {
            this.identifier = identifier;
        }

        public List<Message> getMessages() {
            return messages;
        }

        public ArrayList<Instant> getTimestamps() {
            return timestamps;
        }

        public void addMessage(Message message, Instant timestamp) {
            messages.add(message);
            timestamps.add(timestamp);
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

    private static final class BucketForReader {
        private final Optional<Bucket> bucket;
        private final boolean oldestBucketInQuery;

        private BucketForReader(Bucket bucket, boolean oldestBucketInQuery) {
            this.bucket = ofNullable(bucket);
            this.oldestBucketInQuery = oldestBucketInQuery;
        }

        public MessagesWithTimestamps readMessages() {
            return bucket
                    .map(this::readFromBucket)
                    .orElse(MessagesWithTimestamps.EMPTY);
        }

        private MessagesWithTimestamps readFromBucket(Bucket b) {

            long stamp = b.lock.tryOptimisticRead();

            MessagesWithTimestamps result = MessagesWithTimestamps.createForBucket(b, oldestBucketInQuery);

            if (!b.lock.validate(stamp)) {

                try {
                    stamp = b.lock.readLock();

                    result = MessagesWithTimestamps.createForBucket(b, oldestBucketInQuery);

                } finally {
                    b.lock.unlockRead(stamp);
                }

            }

            return result;
        }

    }

    private static final class MessagesWithTimestamps {
        public static final MessagesWithTimestamps EMPTY = new MessagesWithTimestamps(emptyList(), emptyList(), false);


        private final List<Message> messages;
        private final List<Instant> timestamps;
        private final boolean mayContainMessagesExceededKeepAlive;

        private MessagesWithTimestamps(List<Message> messages, List<Instant> timestamps, boolean mayContainMessagesExceededKeepAlive) {
            this.messages = messages;
            this.timestamps = timestamps;
            this.mayContainMessagesExceededKeepAlive = mayContainMessagesExceededKeepAlive;
        }

        public List<Message> getMessages() {
            return messages;
        }

        public List<Instant> getTimestamps() {
            return timestamps;
        }

        public boolean isEmpty() {
            return messages.isEmpty();
        }

        public int size() {
            return messages.size();
        }

        public boolean mayContainMessagesWithExceededKeepAlive() {
            return mayContainMessagesExceededKeepAlive;
        }

        public static MessagesWithTimestamps createForBucket(Bucket bucket, boolean isOldestBucketInQuery) {
            return new MessagesWithTimestamps(
                    new ArrayList<>(bucket.getMessages()),
                    isOldestBucketInQuery ? new ArrayList<>(bucket.getTimestamps()) : emptyList(),
                    isOldestBucketInQuery);
        }

    }

}

