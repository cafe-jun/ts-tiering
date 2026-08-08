package dev.tstiering.bench;

import dev.tstiering.core.TsValue;

/**
 * 하나의 텔레메트리 키가 시간에 따라 어떤 값을 내는지에 대한 모델.
 *
 * <p><b>이 인터페이스가 Phase 1 에서 가장 중요한 부분이다.</b>
 * 센서 값을 균등 난수로 만들면 Parquet 의 델타 인코딩 / RLE / 딕셔너리가 전혀 먹지 않아
 * 압축률 측정이 통째로 무의미해진다. 실제 센서는
 * <ul>
 *   <li>연속적으로 완만하게 변하고 (온도, 습도) → 델타 인코딩이 먹음</li>
 *   <li>대부분 값이 그대로거나 (가동상태) → RLE 가 먹음</li>
 *   <li>카디널리티가 낮고 (에러코드) → 딕셔너리가 먹음</li>
 *   <li>보고 정밀도가 제한적이다 (소수 1자리) → 유효비트가 적음</li>
 * </ul>
 * 이 특성을 모사하지 않으면 W3 의 압축률 숫자는 버려야 한다.
 *
 * <p>구현은 반드시 <b>무상태</b>여야 한다. (ts, deviceSeed) 만으로 값이 결정되어야
 * 생성 순서를 바꾸거나 병렬화해도 같은 데이터가 나온다.
 */
public interface SensorModel {

    String key();

    TsValue valueAt(long ts, long deviceSeed);
}
