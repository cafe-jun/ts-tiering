package dev.tstiering.archiver;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Properties;

/**
 * archiver 의 컨슈머 설정. <b>ADR-0006 의 2·3번 결정을 코드로 강제한다.</b>
 *
 * <p>이 두 값은 설정이 아니라 계약이다. 잘못 두면 조용히 유실이 나므로
 * 바깥에서 덮어쓸 수 없게 만든다.
 *
 * @param uncommittedSpanLimit 미커밋 오프셋이 이만큼 쌓이면 가장 오래된 슬롯부터 강제로 닫는다.
 *                             파일 품질(이벤트 시각)과 복구 경계(오프셋 lag)는 다른 축인데
 *                             ADR-0005 는 전자만 통제한다 (ADR-0006 결정 6)
 */
public record ArchiverConfig(
        String bootstrapServers,
        String topic,
        String groupId,
        Duration pollTimeout,
        int maxPollRecords,
        long uncommittedSpanLimit
) {

    public static final String DEFAULT_TOPIC = "telemetry";

    public ArchiverConfig {
        if (uncommittedSpanLimit <= 0) {
            throw new IllegalArgumentException("uncommittedSpanLimit must be > 0");
        }
    }

    public static ArchiverConfig local() {
        return new ArchiverConfig("localhost:9092", DEFAULT_TOPIC, "ts-tiering-archiver",
                Duration.ofSeconds(1), 500, 2_000_000);
    }

    public ArchiverConfig withTopic(String newTopic) {
        return new ArchiverConfig(bootstrapServers, newTopic, groupId,
                pollTimeout, maxPollRecords, uncommittedSpanLimit);
    }

    public ArchiverConfig withGroupId(String newGroupId) {
        return new ArchiverConfig(bootstrapServers, topic, newGroupId,
                pollTimeout, maxPollRecords, uncommittedSpanLimit);
    }

    public ArchiverConfig withUncommittedSpanLimit(long limit) {
        return new ArchiverConfig(bootstrapServers, topic, groupId,
                pollTimeout, maxPollRecords, limit);
    }

    /**
     * 컨슈머 속성. <b>여기서 정하는 두 값은 협상 대상이 아니다.</b>
     *
     * <p><b>{@code enable.auto.commit=false}</b> — auto-commit 은 {@code poll()} 안에서
     * {@code position()} 을 커밋한다. 애플리케이션이 그 레코드로 무엇을 했는지 보지 않으므로,
     * 정렬 모드에서 행이 힙에만 있는 이 파이프라인에서는 <b>"읽자마자 커밋"과 정확히 같다.</b>
     * 그 상태로 죽으면 Kafka·로컬·S3 어디에도 없다.
     *
     * <p><b>{@code auto.offset.reset=none}</b> — 커밋 저수위선이 닫기 창만큼 뒤처지므로
     * 보존이 짧으면 커밋한 오프셋이 로그 앞머리에서 잘려나간다. 그때 기본값 {@code latest} 는
     * <b>로그 끝으로 점프하고 그 사이를 예외도 로그도 없이 건너뛴다.</b>
     * W1 에서 3.462% 유실을 "무손실이 cold 계층의 존재 이유"라며 기각해놓고
     * 컨슈머 기본값이 더 큰 유실을 조용히 만드는 것을 허용할 수 없다.
     */
    public Properties consumerProperties() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        return props;
    }
}
