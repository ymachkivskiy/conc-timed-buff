package org.example.ym.concbuff;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getLast;
import static java.util.stream.Collectors.toList;

public class NaiveMessageStorage implements MessageStorage {

    private final Duration keepAliveDuration;

    private List<MessageWithTimestamp> messages = initialList();

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
        MessageWithTimestamp wrappedMsg = MessageWithTimestamp.wrap(message);

        synchronized (this) {
            messages.add(wrappedMsg);
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

        if (quantity <= 0) {
            return Stream.empty();
        }

        final Instant lastAcceptableTimestamp = Instant.now().minus(keepAliveDuration);

        List<MessageWithTimestamp> result;

        synchronized (this){

            if (messages.isEmpty() || lastMessageExceededKeepAliveTime(lastAcceptableTimestamp)) {
                messages = initialList();
                return Stream.empty();
            }else{
                dropMessagesWhichExceededKeepAliveTime(lastAcceptableTimestamp);
                result = internalGetSnapshotOfLast(quantity);
            }

        }

        return result.stream().map(MessageWithTimestamp::getMessage);

    }

    private boolean lastMessageExceededKeepAliveTime(Instant lastAcceptableTimestamp) {
        return getLast(messages).timestamp.isBefore(lastAcceptableTimestamp);
    }

    private void dropMessagesWhichExceededKeepAliveTime(Instant lastAcceptableTimestamp) {

        int firstIdxOfKeepAliveMessage = findFirstAcceptableMessageIndex(lastAcceptableTimestamp);

        if(firstIdxOfKeepAliveMessage >=0 && firstIdxOfKeepAliveMessage < messages.size()){
            List<MessageWithTimestamp> messagesWithinKeepAliveTime = messages.subList(firstIdxOfKeepAliveMessage, messages.size());
            messages = new ArrayList<>(messagesWithinKeepAliveTime.size() * 2);
            messages.addAll(messagesWithinKeepAliveTime);
        }
        else{
            messages = initialList();
        }

    }

    private ArrayList<MessageWithTimestamp> internalGetSnapshotOfLast(int quantity) {
        int startIdx = Math.max(0, messages.size() - quantity);
        return new ArrayList<>(messages.subList(startIdx, messages.size()));
    }

    private int findFirstAcceptableMessageIndex(Instant lastAcceptable) {
        int idx = 0;
        while (idx < messages.size() && messages.get(idx).timestamp.isBefore(lastAcceptable)) {
            idx++;
        }
        return idx;
    }

    private static class MessageWithTimestamp{

        private final Message message;
        private final Instant timestamp;
        private MessageWithTimestamp(Message message, Instant timestamp) {
            this.message = message;
            this.timestamp = timestamp;
        }

        private static MessageWithTimestamp binarySearchTimestamp(Instant timestamp) {
            return new MessageWithTimestamp(null, timestamp);
        }

        private static MessageWithTimestamp wrap(Message message) {
            return new MessageWithTimestamp(message, Instant.now());
        }

        public Message getMessage() {
            return message;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

    }
    private static ArrayList<MessageWithTimestamp> initialList() {
        return new ArrayList<>(1_000);
    }
}
