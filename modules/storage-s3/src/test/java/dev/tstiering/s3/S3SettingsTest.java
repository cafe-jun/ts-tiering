package dev.tstiering.s3;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3SettingsTest {

    /**
     * 5 MiB 미만 파트는 S3 가 completeMultipartUpload 단계에서 거부한다.
     * 그 시점에는 이미 파트를 다 올린 뒤라 시간을 통째로 날린다 — 생성 시점에 막는다.
     */
    @Test
    void rejectsPartSizeBelowS3Minimum() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> S3Settings.localstack("b").withPartSize(1024));
        assertTrue(e.getMessage().contains("partSize"));
    }

    /** 임계값이 파트 크기보다 작으면 파트가 하나뿐인 multipart 가 생긴다. 단일 PUT 보다 느리기만 하다. */
    @Test
    void rejectsThresholdBelowPartSize() {
        assertThrows(IllegalArgumentException.class,
                () -> S3Settings.localstack("b").withMultipartThreshold(1024));
    }

    /** withPartSize 는 임계값이 뒤집히지 않게 함께 밀어올린다. */
    @Test
    void raisingPartSizeAlsoRaisesThreshold() {
        var s = S3Settings.localstack("b").withPartSize(64 * 1024 * 1024);
        assertEquals(64 * 1024 * 1024, s.partSize());
        assertTrue(s.multipartThreshold() >= s.partSize());
    }

    @Test
    void localstackUsesPathStyleButRealAwsDoesNot() {
        assertTrue(S3Settings.localstack("b").pathStyleAccess());

        var real = new S3Settings(null, "ap-northeast-2", "b", null, null,
                S3Settings.DEFAULT_PART_SIZE, S3Settings.DEFAULT_PART_SIZE);
        assertFalse(real.pathStyleAccess());
    }

    @Test
    void rejectsBlankBucket() {
        assertThrows(IllegalArgumentException.class,
                () -> new S3Settings(null, "ap-northeast-2", "  ", null, null,
                        S3Settings.DEFAULT_PART_SIZE, S3Settings.DEFAULT_PART_SIZE));
    }
}
