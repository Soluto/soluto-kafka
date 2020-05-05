package kafka;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import target.ITarget;

public class Consumer {
    private ConsumerFlux<String, String> receiver;
    private final ITarget target;
    private final long processingDelay;

    Consumer(ConsumerFlux<String, String> receiver, ITarget target, long processingDelay) {
        this.receiver = receiver;
        this.target = target;
        this.processingDelay = processingDelay;
    }

    public Flux<?> stream() {
        return receiver
            .onBackpressureBuffer()
            .doFinally(__ -> receiver.dispose())
            .doOnNext(x -> System.out.println("batch size is " + x.count()))
            .doOnRequest(
                requested -> {
                    System.out.println("Requested " + requested);
                    receiver.handleRequest(requested);
                }
            )
            .flatMapIterable(records -> records, 10)
            // .delayElements(Duration.ofMillis(processingDelay))
            .groupBy(x -> x.partition() % 10)
            .flatMap(
                partition -> partition.concatMap(
                    record -> Mono
                        .fromFuture(target.call(record))
                        .doOnSuccess(
                            targetResponse -> {
                                // System.out.println("Http response: " + Thread.currentThread().getName());
                                if (targetResponse.callLatency.isPresent()) {
                                    Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                                }
                                if (targetResponse.resultLatency.isPresent()) {
                                    Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                                }
                            }
                        )
                        .thenEmpty(receiver.commit())
                )
            )
            .onErrorContinue(
                a -> a instanceof CommitFailedException,
                (a, v) -> {
                    System.out.println("commit_failed");
                }
            );
    }
}

class Partitioner {

    Iterable<Iterable<ConsumerRecord<String, String>>> partition(Iterable<ConsumerRecord<String, String>> records) {
        return StreamSupport
            .stream(records.spliterator(), false)
            .collect(Collectors.groupingBy(ConsumerRecord::key))
            .values()
            .stream()
            .map(this::createPartition)
            .collect(Collectors.toList());
    }

    private List<ConsumerRecord<String, String>> createPartition(List<ConsumerRecord<String, String>> consumerRecords) {
        List<ConsumerRecord<String, String>> sorted = consumerRecords
            .stream()
            .sorted(Comparator.comparingLong(ConsumerRecord::offset))
            .collect(Collectors.toList());

        // return Config.DEDUP_PARTITION_BY_KEY ? Collections.singletonList(sorted.get(0)) : sorted;
        return sorted;
    }
}
