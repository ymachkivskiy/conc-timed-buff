package org.example.ym.concbuff;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public interface MessageStorage {

    void storeMessage(Message message);

    List<Message> queryLatest(int quantity);

    int countLatestMatching(int quantity, Predicate<Message> predicate);

    static MessageStorage newStorage(int messagesKeepTime, TimeUnit unit) {
        return ConcurrentMessageStorage.newConcurrentMessageStorage(messagesKeepTime, unit);
    }

}
