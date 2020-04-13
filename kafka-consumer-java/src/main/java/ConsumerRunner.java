import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ConsumerRunner implements IConsumerRunnerLifecycle {
    private final Processor processor;
    private final Partitioner partitioner;
    private KafkaConsumer<String, String> consumer;
    private List<String> topics;
    private Disposable consumerFlowable;

    ConsumerRunner(
        KafkaConsumer<String, String> consumer,
        List<String> topics,
        long processingDelay,
        KafkaProducer<String, String> producer,
        String retryTopic,
        String deadLetterTopic
    ) {
        this.consumer = consumer;
        this.topics = topics;
        this.processor = new Processor(processingDelay, producer, retryTopic, deadLetterTopic);
        this.partitioner = new Partitioner();
    }

    public void start() {
        consumerFlowable =
            Flowable
                .<ConsumerRecords<String, String>>create(
                    emitter -> {
                        consumer.subscribe(topics);
                        while (!emitter.isCancelled()) {
                            emitter.onNext(consumer.poll(Duration.ofMillis(Config.CONSUMER_POLL_TIMEOUT)));
                        }
                        emitter.onComplete();
                    },
                    BackpressureStrategy.DROP
                )
                .onBackpressureDrop(this::monitorDrops)
                .filter(records -> records.count() > 0)
                .doOnNext(this::monitorConsumed)
                .map(records -> partitioner.partition(records))
                .flatMap(processor::process)
                .toList()
                .flatMap(
                    x -> {
                        return Single.create(
                            emitter -> {
                                consumer.commitAsync(
                                    (result, throwable) -> {
                                        if (throwable != null) {
                                            Monitor.commitFailed(throwable);
                                            emitter.onError(throwable);
                                            return;
                                        }
                                        emitter.onSuccess(result);
                                    }
                                );
                            }
                        );
                    }
                )
                .subscribeOn(Schedulers.io())
                .doFinally(() -> consumer.close())
                .subscribe();
    }

    public void stop() {
        try {
            consumerFlowable.dispose();
            Thread.sleep(3 * Config.CONSUMER_POLL_TIMEOUT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void monitorConsumed(ConsumerRecords<String, String> records) {
        Monitor.consumed(records);
    }

    private void monitorDrops(ConsumerRecords<String, String> records) {
        Monitor.monitorDroppedRecords(records.count());
    }
}
