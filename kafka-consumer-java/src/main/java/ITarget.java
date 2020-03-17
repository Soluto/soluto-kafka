import java.util.concurrent.CompletableFuture;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

interface ITarget {
    CompletableFuture<TargetResponse> call(ConsumerRecord<String, String> record);

    default String getOriginalTopic(ConsumerRecord<String, String> record) {
        Iterator<Header> headers = record.headers().headers(Config.ORIGINAL_TOPIC).iterator();
        if (headers.hasNext()) {
            return String.valueOf(headers.next().value());
        }
        return record.topic();
    }
}
