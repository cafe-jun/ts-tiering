package dev.tstiering.s3;

import java.time.Duration;

/**
 * 업로드 한 건의 측정치. W3 의 "파일 크기별 업로드 시간" 표가 이 레코드의 목록에서 나온다.
 *
 * @param key       객체 키
 * @param bytes     업로드한 바이트
 * @param parts     multipart 파트 수. 단일 PUT 이면 1
 * @param multipart multipart 경로를 탔는지
 * @param elapsed   호출 시작부터 완료까지
 */
public record UploadResult(String key, long bytes, int parts, boolean multipart, Duration elapsed) {

    public double mibPerSecond() {
        double seconds = elapsed.toNanos() / 1_000_000_000.0;
        return seconds == 0 ? 0 : (bytes / 1024.0 / 1024.0) / seconds;
    }

    public double mib() {
        return bytes / 1024.0 / 1024.0;
    }
}
