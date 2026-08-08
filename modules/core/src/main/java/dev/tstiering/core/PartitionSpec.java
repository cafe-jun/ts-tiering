package dev.tstiering.core;

/**
 * Datapoint 를 S3 객체 경로로 매핑하는 규칙.
 *
 * <p>W5~W6 벤치마크에서 구현체를 갈아끼우며 스캔량을 비교하는 것이 이 인터페이스의 존재 이유다.
 *
 * <p>조회 시 스캔할 프리픽스를 되돌려주는 메서드({@code prefixesFor})는 일부러 아직 두지 않았다.
 * 어떤 인자가 필요한지는 벤치마크 쿼리 3종을 확정한 뒤에야 알 수 있고,
 * 지금 추측해서 넣으면 3개 구현체 전부가 틀린 시그니처를 떠안는다.
 */
public interface PartitionSpec {

    /** 벤치마크 결과표에 찍히는 이름. 예: "date-hour", "tenant-profile-date-hour" */
    String name();

    /** 객체 키의 디렉터리 부분. 앞뒤 슬래시 없음. 예: {@code tenant=a/date=2026-08-08/hour=14} */
    String path(Datapoint dp);
}
