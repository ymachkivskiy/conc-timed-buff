package org.example.ym.concbuff.benchmark;

import org.example.ym.concbuff.Message;
import org.example.ym.concbuff.NaiveMessageStorage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(value = 2)
@Warmup(
        iterations = 10,
        time = 15,
        timeUnit = TimeUnit.SECONDS
)
@Measurement(
        iterations = 15,
        time = 7,
        timeUnit = TimeUnit.SECONDS
)
public class NaiveMessageStorageBenchmark {


    @State(Scope.Benchmark)
    public static class ThreadSharedBenchmarkState{
        public NaiveMessageStorage storage;

        @Setup(Level.Iteration)
        public void setTupStorage() {
            storage = NaiveMessageStorage.newNaiveMessageStorage(4, TimeUnit.SECONDS);
        }
    }

    @Group("naiveStorageGroup") @GroupThreads(15)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(100)
    public void queryingSnapshots(ThreadSharedBenchmarkState state, Blackhole blackhole) {
        for (int i = 0; i < 100; i++) {
            blackhole.consume(state.storage.countLatestMatching(i * 10, Message::isError));

        }
    }

    @Group("naiveStorageGroup") @GroupThreads(15)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(100)
    public void checkingMessagesWithErrors(ThreadSharedBenchmarkState state, Blackhole blackhole) {
        for (int i = 0; i < 100; i++) {
            blackhole.consume(state.storage.queryLatest(i * 10));
        }
    }


    @Group("naiveStorageGroup") @GroupThreads(25)
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(100)
    public void storingMessages(ThreadSharedBenchmarkState state) {
        for (int i = 0; i < 100; i++) {
            state.storage.storeMessage(new Message("agent-" + i, i));
        }
    }

}
