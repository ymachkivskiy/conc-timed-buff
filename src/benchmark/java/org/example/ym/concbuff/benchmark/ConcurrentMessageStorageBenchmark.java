package org.example.ym.concbuff.benchmark;

import java.util.concurrent.TimeUnit;
import org.example.ym.concbuff.Message;
import org.example.ym.concbuff.ConcurrentMessageStorage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

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
public class ConcurrentMessageStorageBenchmark
{


    @State(Scope.Benchmark)
    public static class ThreadSharedBenchmarkState{
        public ConcurrentMessageStorage storage;

        @Setup(Level.Iteration)
        public void setTupStorage() {
            storage = ConcurrentMessageStorage.newConcurrentMessageStorage(4, TimeUnit.SECONDS);
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
