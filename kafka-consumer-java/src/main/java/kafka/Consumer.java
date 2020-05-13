package kafka;

import configuration.Config;
import java.time.Duration;
import monitoring.Monitor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import target.ITarget;

public class Consumer {
    private ReactiveKafkaClient<String, String> kafkaConsumer;
    private final ITarget target;

    public Consumer(ReactiveKafkaClient<String, String> kafkaConsumer, ITarget target) {
        this.kafkaConsumer = kafkaConsumer;
        this.target = target;
    }

    public Flux<?> stream() {
        return kafkaConsumer
            .doOnNext(records -> System.out.println("start batch"))
            .concatMap(
                records -> Flux
                    .fromIterable(records)
                    .groupBy(x -> x.partition())
                    .delayElements(Duration.ofMillis(Config.PROCESSING_DELAY))
                    .publishOn(Schedulers.parallel())
                    .flatMap(
                        partition -> partition
                            .doOnNext(record -> Monitor.receivedRecord(record))
                            .concatMap(
                                record -> Mono
                                    .fromFuture(target.call(record))
                                    .doOnSuccess(
                                        targetResponse -> {
                                            if (targetResponse.callLatency.isPresent()) {
                                                Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                                            }
                                            if (targetResponse.resultLatency.isPresent()) {
                                                Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                                            }
                                        }
                                    )
                            )
                    )
                    .collectList()
            )
            .doOnNext(__ -> System.out.println("batch completed"))
            .map(
                __ -> {
                    kafkaConsumer.commit();
                    System.out.println("commit completed!!!");
                    return 0;
                }
            )
            .doOnNext(
                __ -> {
                    System.out.println("polling again!!!");
                    kafkaConsumer.poll(Long.valueOf(Config.MAX_POLL_RECORDS));
                }
            )
            .doOnSubscribe(
                __ -> {
                    kafkaConsumer.poll(Long.valueOf(Config.MAX_POLL_RECORDS));
                }
            )
            .onErrorContinue(a -> a instanceof CommitFailedException, (a, v) -> {});
    }
}
