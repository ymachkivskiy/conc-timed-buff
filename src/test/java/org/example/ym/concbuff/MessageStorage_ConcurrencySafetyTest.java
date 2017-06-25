package org.example.ym.concbuff;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.Iterables.getFirst;
import static java.util.stream.Collectors.toList;
import static org.fest.assertions.Assertions.assertThat;

@Category(ConcurrencySafety.class)
public class MessageStorage_ConcurrencySafetyTest {

    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(30, TimeUnit.SECONDS);
    }


    //region MessageStorage::queryLatest(int) tests

    //region Basic tests

    @Test
    public void allReadersShouldHaveSameAccumulatedMessagesChecksumAsCombinedCheckSumForAllWritersStoredMessages() throws BrokenBarrierException, InterruptedException {
        //given
        final MessageStorage storage = MessageStorage.newStorage(30, TimeUnit.MINUTES);

        final int
                THREADS_COUNT = getThreadsCount(),
                MESSAGES_PER_WRITER = 100_000;

        final CyclicBarrier
                executionSynchBarrier = new CyclicBarrier(THREADS_COUNT * 2 + 1),
                allMessagesHaveBeenStored = new CyclicBarrier(THREADS_COUNT * 2);

        final AtomicInteger writersChecksum = new AtomicInteger(0);
        final ConcurrentHashMap<Integer, Boolean> readersCheckSums = new ConcurrentHashMap<>();

        class Writer extends MessageDealingWorker {
            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {
                executionSynchBarrier.await();

                int localCheckSum = 0;
                for (Message errorMessage : generateMessages(MESSAGES_PER_WRITER, this::message)) {
                    storage.storeMessage(errorMessage);
                    localCheckSum = updateCheckSum(localCheckSum, errorMessage.hashCode());
                }

                allMessagesHaveBeenStored.await();

                final int finalLocalCheckSum = localCheckSum;
                writersChecksum.getAndUpdate(oldSum -> updateCheckSum(oldSum, finalLocalCheckSum));

                executionSynchBarrier.await();
            }

        }

        class Reader extends MessageDealingReader {
            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {
                executionSynchBarrier.await();

                performDummyQueryLatestToInterfereWithWriter(storage);

                allMessagesHaveBeenStored.await();

                Set<Integer> localCheckSums = new HashSet<>();
                for (int i = 0; i < 10; i++) {
                    int currentSnapshotChecksum = 0;
                    List<Message> messages = storage.queryLatest(Integer.MAX_VALUE);
                    for (Message message : messages) {
                        currentSnapshotChecksum = updateCheckSum(currentSnapshotChecksum, message.hashCode());
                    }
                    localCheckSums.add(currentSnapshotChecksum);
                }

                for (Integer localCheckSum : localCheckSums) {
                    readersCheckSums.putIfAbsent(localCheckSum, Boolean.TRUE);
                }

                executionSynchBarrier.await();
            }
        }

        //when
        for (int i = 0; i < THREADS_COUNT; i++) {
            executor.execute(new Writer());
            executor.execute(new Reader());
        }

        executionSynchBarrier.await(); // start all threads work when they are ready
        executionSynchBarrier.await(); // wait for all threads to finish

        //then
        assertThat(readersCheckSums).hasSize(1); // all checksums identical
        assertThat(writersChecksum.get()).isEqualTo(getFirst(readersCheckSums.keySet(), 0)); // writers and readers checksums same
    }

    //endregion

    //region Message keep-alive tests

    @Test
    public void allReadersShouldHaveSameAccumulatedMessagesChecksumAsCombinedCheckSumForAllWritersStoredMessages_WithinKeepAliveTime() throws BrokenBarrierException, InterruptedException {
        //given
        final MessageStorage storage = MessageStorage.newStorage(20, TimeUnit.SECONDS);

        final int
                THREADS_COUNT = getThreadsCount(),
                MESSAGES_PER_WRITER_WHICH_EXCEED_KEEP_ALIVE = 50_000,
                MESSAGES_PER_WRITER_DURING_KEEP_ALIVE = 75_000;


        final CyclicBarrier
                executionSynchBarrier = new CyclicBarrier(THREADS_COUNT * 2 + 1),
                keepAliveExceededBarrier = new CyclicBarrier(THREADS_COUNT * 2 + 1),
                allMessagesHaveBeenStored = new CyclicBarrier(THREADS_COUNT * 2);

        final AtomicInteger writersChecksum = new AtomicInteger(0);
        final ConcurrentHashMap<Integer, Boolean> readersCheckSums = new ConcurrentHashMap<>();

        class Writer extends MessageDealingWorker {
            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {
                executionSynchBarrier.await();

                for (Message message : generateMessages(MESSAGES_PER_WRITER_WHICH_EXCEED_KEEP_ALIVE, this::message)) {
                    storage.storeMessage(message);
                }

                keepAliveExceededBarrier.await();

                int localCheckSum = 0;
                for (Message errorMessage : generateMessages(MESSAGES_PER_WRITER_DURING_KEEP_ALIVE, this::message)) {
                    storage.storeMessage(errorMessage);
                    localCheckSum = updateCheckSum(localCheckSum, errorMessage.hashCode());
                }

                allMessagesHaveBeenStored.await();

                final int finalLocalCheckSum = localCheckSum;
                writersChecksum.getAndUpdate(oldSum -> updateCheckSum(oldSum, finalLocalCheckSum));

                executionSynchBarrier.await();
            }

        }

        class Reader extends MessageDealingReader {
            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {
                executionSynchBarrier.await();

                performDummyQueryLatestToInterfereWithWriter(storage);

                keepAliveExceededBarrier.await();

                performDummyQueryLatestToInterfereWithWriter(storage);

                allMessagesHaveBeenStored.await();


                Set<Integer> localCheckSums = new HashSet<>();
                for (int i = 0; i < 10; i++) {
                    int currentSnapshotChecksum = 0;
                    List<Message> messages = storage.queryLatest(Integer.MAX_VALUE);
                    for (Message message : messages) {
                        currentSnapshotChecksum = updateCheckSum(currentSnapshotChecksum, message.hashCode());
                    }
                    localCheckSums.add(currentSnapshotChecksum);
                }
                for (Integer localCheckSum : localCheckSums) {
                    readersCheckSums.putIfAbsent(localCheckSum, Boolean.TRUE);
                }


                executionSynchBarrier.await();
            }
        }

        //when
        for (int i = 0; i < THREADS_COUNT; i++) {
            executor.execute(new Writer());
            executor.execute(new Reader());
        }

        executionSynchBarrier.await(); // start all threads work when they are ready

        TimeUnit.SECONDS.sleep(20 + 1);
        keepAliveExceededBarrier.await(); // keep alive time for old messages has exceeded

        executionSynchBarrier.await(); // wait for all threads to finish

        //then
        assertThat(readersCheckSums).hasSize(1); // all checksums identical
        assertThat(writersChecksum.get()).isEqualTo(getFirst(readersCheckSums.keySet(), 0)); // writers and readers checksums same
    }

    //endregion

    //endregion


    //region MessageStorage::countLatestMatching(int,Predicate<Message>) tests

    //region Basic tests

    @Test
    public void allReadersShouldSeeSameMessageMatchingPredicateCountAsAllWritersStoredSuchMessages() throws BrokenBarrierException, InterruptedException {
        //given
        final MessageStorage storage = MessageStorage.newStorage(30, TimeUnit.MINUTES);

        final int
                THREADS_COUNT = getThreadsCount(),
                ERRORS_PER_WRITER = 25_000,
                NON_ERROR_MESSAGES_PER_WRITER = 100_000;

        final CyclicBarrier
                executionSynchBarrier = new CyclicBarrier(THREADS_COUNT * 2 + 1),
                allErrorsHaveBeenStored = new CyclicBarrier(THREADS_COUNT * 2);

        final AtomicBoolean unexpectedErrorsCountSeen = new AtomicBoolean(false);

        class Writer extends MessageDealingWorker {
            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {

                int nonErrorMessagesMixedToErrorOnes = getRand().nextInt(1 + NON_ERROR_MESSAGES_PER_WRITER / 3);

                List<Message> messagesWithErrorsAndNonErrors = createErrorMessagesShuffledWithNonErrorMessages(ERRORS_PER_WRITER, nonErrorMessagesMixedToErrorOnes);

                executionSynchBarrier.await();

                for (Message errorMessage : messagesWithErrorsAndNonErrors) {
                    storage.storeMessage(errorMessage);
                }

                allErrorsHaveBeenStored.await();

                for (int i = 0; i < NON_ERROR_MESSAGES_PER_WRITER - nonErrorMessagesMixedToErrorOnes; i++) {
                    storage.storeMessage(message());
                }

                executionSynchBarrier.await();
            }

        }

        class Reader extends MessageDealingReader {
            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {
                executionSynchBarrier.await();

                performDummyCountLatestToInterfereWithWriter(storage);

                allErrorsHaveBeenStored.await();

                boolean unexpectedErrorMessageCountNoticed = false;
                for (int i = 0; i < 10; i++) {
                    int actualMessagesWithError = storage.countLatestMatching(Integer.MAX_VALUE, Message::isError);

                    if (actualMessagesWithError != THREADS_COUNT * ERRORS_PER_WRITER) {
                        unexpectedErrorMessageCountNoticed = true;
                    }
                }
                unexpectedErrorsCountSeen.compareAndSet(false, unexpectedErrorMessageCountNoticed);

                executionSynchBarrier.await();
            }
        }

        //when
        for (int i = 0; i < THREADS_COUNT; i++) {
            executor.execute(new Writer());
            executor.execute(new Reader());
        }

        executionSynchBarrier.await(); // start all threads work when they are ready
        executionSynchBarrier.await(); // wait for all threads to finish

        //then
        assertThat(unexpectedErrorsCountSeen.get()).isFalse();
    }
    
    //endregion

    //region Message keep-alive tests

    @Test
    public void allReadersShouldSeeSameMessageMatchingPredicateCount_WithinKeepAlivePeriod() throws BrokenBarrierException, InterruptedException {
        //given
        final MessageStorage storage = MessageStorage.newStorage(10, TimeUnit.SECONDS);

        final int
                THREADS_COUNT = getThreadsCount(),
                ERRORS_PER_WRITER_WHICH_EXCEED_KEEP_ALIVE = 15_000,
                ERRORS_PER_WRITER_DURING_KEEP_ALIVE = 15_000,
                NON_ERROR_MESSAGES_PER_WRITER = 100_000;

        final CyclicBarrier
                executionSynchBarrier = new CyclicBarrier(THREADS_COUNT * 2 + 1),
                keepAliveExceededBarrier = new CyclicBarrier(THREADS_COUNT * 2 + 1),
                allErrorsHaveBeenStored = new CyclicBarrier(THREADS_COUNT * 2);


        final AtomicBoolean unexpectedErrorsCountSeen = new AtomicBoolean(false);

        class Writer extends MessageDealingWorker {
            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {

                List<Message> messagesExceededKeepAlive = createErrorMessagesShuffledWithNonErrorMessages(ERRORS_PER_WRITER_WHICH_EXCEED_KEEP_ALIVE, NON_ERROR_MESSAGES_PER_WRITER / 3);
                List<Message> messagesDuringKeepAlive = createErrorMessagesShuffledWithNonErrorMessages(ERRORS_PER_WRITER_DURING_KEEP_ALIVE, NON_ERROR_MESSAGES_PER_WRITER / 3);

                executionSynchBarrier.await();


                for (Message message : messagesExceededKeepAlive) {
                    storage.storeMessage(message);
                }

                keepAliveExceededBarrier.await();


                for (Message message : messagesDuringKeepAlive) {
                    storage.storeMessage(message);
                }

                allErrorsHaveBeenStored.await();

                for (int i = 0; i < NON_ERROR_MESSAGES_PER_WRITER / 3; i++) {
                    storage.storeMessage(message());
                }

                executionSynchBarrier.await();
            }
        }

        class Reader extends MessageDealingReader {

            @Override
            public void runInternal() throws BrokenBarrierException, InterruptedException {
                executionSynchBarrier.await();

                performDummyCountLatestToInterfereWithWriter(storage);

                keepAliveExceededBarrier.await();

                performDummyCountLatestToInterfereWithWriter(storage);

                allErrorsHaveBeenStored.await();

                boolean unexpectedErrorMessageCountNoticed = false;
                for (int i = 0; i < 10; i++) {
                    int actualMessagesWithError = storage.countLatestMatching(Integer.MAX_VALUE, Message::isError);

                    if (actualMessagesWithError != THREADS_COUNT * ERRORS_PER_WRITER_DURING_KEEP_ALIVE) {
                        unexpectedErrorMessageCountNoticed = true;
                    }
                }
                unexpectedErrorsCountSeen.compareAndSet(false, unexpectedErrorMessageCountNoticed);

                executionSynchBarrier.await();
            }

        }

        //when
        for (int i = 0; i < THREADS_COUNT; i++) {
            executor.execute(new Writer());
            executor.execute(new Reader());
        }

        executionSynchBarrier.await(); // start all threads work when they are ready

        TimeUnit.MILLISECONDS.sleep(10_000 + 500);
        keepAliveExceededBarrier.await(); // keep alive time for old messages has exceeded

        executionSynchBarrier.await(); // wait for all threads to finish

        //then
        assertThat(unexpectedErrorsCountSeen.get()).isFalse();
    }

    //endregion

    //endregion


    //region Helper functions and classes

    private abstract static class MessageDealingReader extends MessageDealingWorker {
        public volatile int blackHolle;
        public volatile Object objectBh;

        protected final void performDummyCountLatestToInterfereWithWriter(MessageStorage storage) {
            // dummy interfering writers
            for (int i = 0; i < 100; i++) {
                blackHolle = storage.countLatestMatching(100, Message::isError);
            }
        }

        protected final void performDummyQueryLatestToInterfereWithWriter(MessageStorage storage) {
            for (int i = 0; i < 50; i++) {
                objectBh = storage.queryLatest(1_000);
            }
        }
    }

    private abstract static class MessageDealingWorker implements Runnable {

        private Random r;

        @Override
        public final void run() {
            r = new Random(Thread.currentThread().getId() ^ System.nanoTime());

            try {
                runInternal();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        protected abstract void runInternal() throws InterruptedException, BrokenBarrierException;

        final Random getRand() {
            return r;
        }

        protected final List<Message> generateMessages(int count, Supplier<Message> messageSupplier) {
            return IntStream.range(0, count)
                    .mapToObj(i -> messageSupplier.get())
                    .collect(toList());
        }

        protected final Message message() {
            String agentString = Thread.currentThread().getName() + Instant.now().toString();
            return new Message(agentString, r.nextInt(350));
        }

        protected final Message errorMessage() {
            String agentString = Thread.currentThread().getName() + Instant.now().toString() + r.nextInt();
            return new Message(agentString, 400 + r.nextInt(150));
        }

        protected final List<Message> createErrorMessagesShuffledWithNonErrorMessages(int errorMessagesCount, int nonErrorMessagesCount) {
            List<Message> messagesWithErrorsAndNonErrors = new ArrayList<>();
            messagesWithErrorsAndNonErrors.addAll(generateMessages(errorMessagesCount, this::errorMessage));
            messagesWithErrorsAndNonErrors.addAll(generateMessages(nonErrorMessagesCount, this::message));
            Collections.shuffle(messagesWithErrorsAndNonErrors, getRand());
            return messagesWithErrorsAndNonErrors;
        }
    }

    private static int getThreadsCount() {
        return Runtime.getRuntime().availableProcessors() * 3;
    }

    private static int updateCheckSum(int currentCheckSum, int newValue) {
        return currentCheckSum ^ newValue;
    }


    //endregion


}