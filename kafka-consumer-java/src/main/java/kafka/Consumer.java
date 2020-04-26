package kafka;

import configuration.Config;
import io.reactivex.*;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;
import monitoring.Monitor;
import reactor.kafka.receiver.ReceiverRecord;
import target.ITarget;
import target.TargetResponse;

public class Consumer {
    private Flowable<ReceiverRecord<String, String>> receiver;
    private final ITarget target;
    private final long processingDelay;

    Consumer(Flowable<ReceiverRecord<String, String>> receiver, ITarget target, long processingDelay) {
        this.receiver = receiver;
        this.target = target;
        this.processingDelay = processingDelay;
    }

    public Flowable<?> stream() {
        return receiver
            .observeOn(Schedulers.io(), false, Config.BUFFER_SIZE)
            .doOnNext(
                record -> {
                    System.out.println("On Next: " + Thread.currentThread().getName());
                    Monitor.receivedRecord(record);
                }
            )
            .delay(processingDelay, TimeUnit.MILLISECONDS)
            .groupBy(
                record -> {
                    System.out.println("Group by: " + Thread.currentThread().getName());
                    return record.partition();
                }
            )
            .flatMap(
                partition -> partition
                    .observeOn(Schedulers.io())
                    .concatMap(
                        record -> Flowable
                            .fromFuture(target.call(record))
                            .doOnNext(
                                targetResponse -> {
                                    if (targetResponse.callLatency.isPresent()) {
                                        Monitor.callTargetLatency(targetResponse.callLatency.getAsLong());
                                    }
                                    if (targetResponse.resultLatency.isPresent()) {
                                        Monitor.resultTargetLatency(targetResponse.resultLatency.getAsLong());
                                    }
                                }
                            )
                            .map(__ -> record)
                    )
                    .concatMap(
                        record -> Flowable.fromCallable(
                            () -> {
                                record.receiverOffset().acknowledge();
                                return 0;
                            }
                        )
                    )
            );
    }
}
