package org.example.ym.concbuff;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class MessageStorage_FunctionalTest {

    //region MessageStorage::queryLatest(int) tests

    //region Basic tests

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnEmptyListWhenThereIsNoMessagesInStorage() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        //when
        List<Message> messages = storage.queryLatest(3);

        //then
        assertThat(messages).isEmpty();
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnEmptyListWhenZeroMessagesQueried() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(message());

        //when
        List<Message> messages = storage.queryLatest(0);

        //then
        assertThat(messages).isEmpty();
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnStoredMessagesInOrderTheyWereStored() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        Message m1 = message(),
                m2 = message(),
                m3 = message();

        storage.storeMessage(m1);
        storage.storeMessage(m2);
        storage.storeMessage(m3);

        //when
        List<Message> messages = storage.queryLatest(3);

        //then
        assertThat(messages).containsExactly(m1, m2, m3);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnOnlyRequestedQuantityOfLatestStoredMessagesIfAvailable() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        Message m1 = message(),
                m2 = message(),
                messageBeforeLast = message(),
                lastMessage = message();

        //when
        storage.storeMessage(m1);
        storage.storeMessage(m2);
        storage.storeMessage(messageBeforeLast);
        storage.storeMessage(lastMessage);

        List<Message> messages = storage.queryLatest(2);

        //then
        assertThat(messages).containsExactly(messageBeforeLast, lastMessage);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnAllStoredMessagesIfRequestedQuantityIsGreaterThanStoredMessagesCount() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        Message m1 = message(),
                m2 = message(),
                m3 = message();

        //when
        storage.storeMessage(m1);
        storage.storeMessage(m2);
        storage.storeMessage(m3);

        List<Message> messages = storage.queryLatest(10);

        //then
        assertThat(messages).containsExactly(m1, m2, m3);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnSameMessagesForSubsequentQuerying() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(message());
        storage.storeMessage(message());

        //when

        List<Message> firstQueryMessages = storage.queryLatest(2);
        List<Message> secondQueryMessages = storage.queryLatest(2);
        List<Message> thirdQueryMessages = storage.queryLatest(2);

        //then
        assertThat(firstQueryMessages).isEqualTo(secondQueryMessages);
        assertThat(secondQueryMessages).isEqualTo(thirdQueryMessages);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnSnapshotOfStoredMessages() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        Message m1 = message(),
                m2 = message(),
                m3 = message();

        storage.storeMessage(m1);
        storage.storeMessage(m2);
        storage.storeMessage(m3);

        //when
        List<Message> firstQuerySnapshotMessages = storage.queryLatest(3);

        firstQuerySnapshotMessages.add(message());
        firstQuerySnapshotMessages.remove(0);
        firstQuerySnapshotMessages.add(0, message());

        List<Message> secondQueryMessages = storage.queryLatest(3);

        //then
        assertThat(secondQueryMessages).containsExactly(m1, m2, m3);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldBeAbleToStoreHugeAmountOfMessages() {
        //given
        MessageStorage storage = MessageStorage.newStorage(10, TimeUnit.MINUTES);

        final int AMOUNT = 1_000_000;

        List<Message> expectedMessagesToStore = generateRandomMessages(AMOUNT);

        storeAllMessages(storage, expectedMessagesToStore);

        //when
        List<Message> actualStoredMessages = storage.queryLatest(AMOUNT);

        //then
        // do not produce huge console output by printing expected and actual lists when not match
        assertTrue(actualStoredMessages.equals(expectedMessagesToStore));
    }

    //endregion

    //region Message keep-alive tests

    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnEmptyListWhenAllStoredMessagesHaveExpiredKeepAliveTime() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.SECONDS);

        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(message());

        //when
        TimeUnit.SECONDS.sleep(2);

        List<Message> messages = storage.queryLatest(3);

        //then
        assertThat(messages).isEmpty();
    }

    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnTwoLatestMessagesFromKeepAlivePeriod() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(3, TimeUnit.SECONDS);

        storage.storeMessage(message());
        storage.storeMessage(message());

        TimeUnit.MILLISECONDS.sleep(1_500 + 250);

        Message firstMessage = message(),
                lastMessage = message();

        storage.storeMessage(firstMessage);
        storage.storeMessage(lastMessage);

        TimeUnit.MILLISECONDS.sleep(1_500);

        //when
        List<Message> messages = storage.queryLatest(5);

        //then
        assertThat(messages).containsExactly(firstMessage, lastMessage);
    }

    @Ignore("To granular keep alive time")
    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnOnlyMessagesFromKeepAlivePeriod_microsecondsGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(100, TimeUnit.MILLISECONDS);

        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(message());

        TimeUnit.MICROSECONDS.sleep(50_000);

        Message m1 = message(),
                m2 = message();

        storage.storeMessage(m1);
        storage.storeMessage(m2);

        TimeUnit.MICROSECONDS.sleep(50_000 + 1);

        //when
        List<Message> messages = storage.queryLatest(5);

        //then
        assertThat(messages).containsExactly(m1, m2);
    }

    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnOnlyMessagesFromKeepAlivePeriod_millisecondsGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(2, TimeUnit.SECONDS);

        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(message());

        TimeUnit.MILLISECONDS.sleep(1_000);

        Message m1 = message(),
                m2 = message();

        storage.storeMessage(m1);
        storage.storeMessage(m2);

        TimeUnit.MILLISECONDS.sleep(1_000 + 1);

        //when
        List<Message> messages = storage.queryLatest(5);

        //then
        assertThat(messages).containsExactly(m1, m2);
    }

    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnOnlyMessagesFromKeepAlivePeriod_secondsGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(5, TimeUnit.SECONDS);

        storeRandomMessages(storage, 4);

        TimeUnit.SECONDS.sleep(3);

        Message m1 = message(),
                m2 = message(),
                m3 = message();

        storage.storeMessage(m1);
        storage.storeMessage(m2);
        storage.storeMessage(m3);

        TimeUnit.SECONDS.sleep(3);

        //when
        List<Message> messages = storage.queryLatest(15);

        //then
        assertThat(messages).containsExactly(m1, m2, m3);
    }

    @Ignore("To consider if this is valuable test")
    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnOnlyMessagesFromKeepAlivePeriod_minutesGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storeRandomMessages(storage, 10);

        TimeUnit.SECONDS.sleep(30);

        Message m1 = message(),
                m2 = message(),
                m3 = message(),
                m4 = message();

        storage.storeMessage(m1);
        storage.storeMessage(m2);

        TimeUnit.SECONDS.sleep(31);

        storage.storeMessage(m3);
        storage.storeMessage(m4);

        //when
        List<Message> messages = storage.queryLatest(23);

        //then
        assertThat(messages).containsExactly(m1, m2, m3, m4);
    }

    //endregion

    //endregion


    //region MessageStorage::countLatestMatching(int,Predicate<Message>) tests

    //region Basic tests

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnNumberOfMessagesMatchingPredicate() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());

        //when
        int errorMessageCount = storage.countLatestMatching(5, Message::isError);

        //then
        assertThat(errorMessageCount).isEqualTo(3);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnZeroIfThereAreNoStoredMessages() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        //when
        int errorMessageCount = storage.countLatestMatching(100, Message::isError);

        //then
        assertThat(errorMessageCount).isEqualTo(0);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnZeroIfZeroLastMessageWereQueried() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());

        //when
        int errorMessageCount = storage.countLatestMatching(0, Message::isError);

        //then
        assertThat(errorMessageCount).isEqualTo(0);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnZeroIfThereAreNoMessagesMatchingPredicate() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(message());

        //when
        int errorMessageCount = storage.countLatestMatching(10, Message::isError);

        //then
        assertThat(errorMessageCount).isEqualTo(0);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnNumberOfMessagesMatchingPredicateIfRequestedQuantityIs_Greater_ThanStoredMessagesCount() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());

        //when
        int errorMessageCount = storage.countLatestMatching(100, Message::isError);

        //then
        assertThat(errorMessageCount).isEqualTo(3);
    }

    @Category(BasicFunctionality.class)
    @Test
    public void shouldReturnNumberOfMessagesMatchingPredicateIfRequestedQuantityIs_Lesser_ThanStoredMessagesCount() {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        // -----------
        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());

        //when
        int errorMessageCount = storage.countLatestMatching(5, Message::isError);

        //then
        assertThat(errorMessageCount).isEqualTo(2);
    }

    //endregion

    //region Message keep-alive tests

    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnNumberOfLatestMessagesMatchingPredicateOnlyFromKeepAlivePeriod() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(3, TimeUnit.SECONDS);

        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());

        TimeUnit.MILLISECONDS.sleep(1_500 + 250);

        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());

        TimeUnit.MILLISECONDS.sleep(1_500);

        //when
        int messagesWithError = storage.countLatestMatching(5, Message::isError);

        //then
        assertThat(messagesWithError).isEqualTo(2);
    }

    @Ignore("To granular keep alive time")
    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnNumberOfLatestMessagesMatchingPredicateOnlyFromKeepAlivePeriod_microsecondsGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(100, TimeUnit.MILLISECONDS);

        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());

        TimeUnit.MICROSECONDS.sleep(50_000);

        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());

        TimeUnit.MICROSECONDS.sleep(50_000 + 1);

        //when
        int messagesWithError = storage.countLatestMatching(5, Message::isError);

        //then
        assertThat(messagesWithError).isEqualTo(2);
    }

    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnNumberOfLatestMessagesMatchingPredicateOnlyFromKeepAlivePeriod_millisecondsGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(2, TimeUnit.SECONDS);

        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());

        TimeUnit.MILLISECONDS.sleep(1_000);

        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());

        TimeUnit.MILLISECONDS.sleep(1_000 + 1);

        //when
        int messagesWithError = storage.countLatestMatching(5, Message::isError);

        //then
        assertThat(messagesWithError).isEqualTo(2);
    }

    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnNumberOfLatestMessagesMatchingPredicateOnlyFromKeepAlivePeriod_secondsGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(5, TimeUnit.SECONDS);

        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());

        TimeUnit.SECONDS.sleep(3);

        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(message());

        TimeUnit.SECONDS.sleep(3);

        //when
        int messagesWithError = storage.countLatestMatching(100, Message::isError);

        //then
        assertThat(messagesWithError).isEqualTo(1);
    }

    @Ignore("To consider if this is valuable test")
    @Category(MessageKeepAliveFunctionality.class)
    @Test
    public void shouldReturnNumberOfLatestMessagesMatchingPredicateOnlyFromKeepAlivePeriod_minutesGranularity() throws InterruptedException {
        //given
        MessageStorage storage = MessageStorage.newStorage(1, TimeUnit.MINUTES);

        storage.storeMessage(message());
        storage.storeMessage(errorMessage());
        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());

        TimeUnit.SECONDS.sleep(30);

        storage.storeMessage(errorMessage());
        storage.storeMessage(message());
        storage.storeMessage(message());
        storage.storeMessage(errorMessage());

        TimeUnit.SECONDS.sleep(31);

        //when
        int messagesWithError = storage.countLatestMatching(100, Message::isError);

        //then
        assertThat(messagesWithError).isEqualTo(2);
    }

    //endregion

    //endregion




    //region Helper functions

    private static final Random r = new Random();


    private static void storeAllMessages(MessageStorage storage, List<Message> expectedMessagesToStore) {
        for (Message message : expectedMessagesToStore) {
            storage.storeMessage(message);
        }
    }

    private static void storeRandomMessages(MessageStorage storage, int amount) {
        storeAllMessages(storage, generateRandomMessages(amount));
    }

    private static List<Message> generateRandomMessages(int amount) {
        return IntStream.range(0, amount)
                .mapToObj(i -> message())
                .collect(toList());
    }

    private static Message message() {
        return messageWithCode(r.nextInt(300));
    }

    private static Message errorMessage() {
        return messageWithCode(400 + r.nextInt(150));
    }

    private static Message messageWithCode(int responseCode) {
        return new Message("agent-" + UUID.randomUUID().toString().substring(0, 5), responseCode);
    }


    //endregion

}
