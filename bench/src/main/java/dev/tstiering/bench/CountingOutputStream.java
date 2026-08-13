package dev.tstiering.bench;

import java.io.OutputStream;

/**
 * 바이트를 버리고 개수만 센다. NDJSON 기준선을 디스크에 만들지 않고 크기만 얻기 위한 것.
 *
 * <p>버퍼링하지 않는다 — Jackson 이 이미 자체 버퍼로 모아서 큰 덩어리로 넘긴다.
 */
public final class CountingOutputStream extends OutputStream {

    private long bytes;

    @Override
    public void write(int b) {
        bytes++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        bytes += len;
    }

    public long bytes() {
        return bytes;
    }
}
