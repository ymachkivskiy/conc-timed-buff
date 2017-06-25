package org.example.ym.concbuff;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class NaiveMessageStorage implements MessageStorage {

    private final Duration keepAliveDuration;

    private List<MessageWithTimestamp> messages = new ArrayList<>();

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
        messages.add(MessageWithTimestamp.wrapp(message));
    }

    @Override
    public List<Message> queryLatest(int quantity) {
        final Instant lastAcceptable = Instant.now().minus(keepAliveDuration);

        if (quantity <= 0 || messages.isEmpty()) {
            return Collections.emptyList();
        }

        int startIdx = 0;
        while (startIdx < messages.size() && messages.get(startIdx).timestamp.isBefore(lastAcceptable)) {
            startIdx++;
        }

        List<MessageWithTimestamp> result;
        if(startIdx < messages.size()){
            messages = new ArrayList<>(messages.subList(startIdx, messages.size()));
            result = messages.subList(Math.max(0, messages.size() - quantity), messages.size());
        }
        else{
            messages = new ArrayList<>();
            result = Collections.emptyList();
        }

        return result.stream().map(MessageWithTimestamp::getMessage).collect(toList());
    }

    @Override
    public int countLatestMatching(int quantity, Predicate<Message> predicate) {
        return (int) queryLatest(quantity).stream().filter(predicate).count();
    }

    private static class MessageWithTimestamp{
        private final Message message;
        private final Instant timestamp;

        private MessageWithTimestamp(Message message, Instant timestamp) {
            this.message = message;
            this.timestamp = timestamp;
        }

        private static MessageWithTimestamp wrapp(Message message) {
            return new MessageWithTimestamp(message, Instant.now());
        }

        public Message getMessage() {
            return message;
        }
    }
}
