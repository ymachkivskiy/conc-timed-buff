package org.example.ym.concbuff;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;

public class NaiveMessageStorage implements MessageStorage {



    private NaiveMessageStorage(int messagesKeepTime, TimeUnit unit) {

    }

    public static NaiveMessageStorage newNaiveMessageStorage(int messagesKeepTime, TimeUnit unit) {
        checkArgument(unit != TimeUnit.NANOSECONDS && unit != TimeUnit.MICROSECONDS, "To small time unit " + unit.name());
        checkArgument(unit != TimeUnit.DAYS, "To large time unit");

        return new NaiveMessageStorage(messagesKeepTime, unit);
    }

    @Override
    public void storeMessage(Message message) {

    }

    @Override
    public List<Message> queryLatest(int quantity) {
        return Collections.emptyList();
    }

    @Override
    public int countLatestMatching(int quantity, Predicate<Message> predicate) {
        return 0;
    }
}
