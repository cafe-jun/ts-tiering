package dev.tstiering.archiver;

import dev.tstiering.parquet.ParquetStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * 로컬 파일의 상태를 <b>경로로 드러낸다</b> (ADR-0006 결정 7).
 *
 * <pre>
 * &lt;root&gt;/inflight/&lt;파티션경로&gt;/....parquet   ← 쓰는 중. 언제든 폐기 가능
 * &lt;root&gt;/ready/&lt;파티션경로&gt;/....parquet      ← close 성공. 업로드 후보
 * </pre>
 *
 * <p>상태를 메모리에 두면 재시작 시 반드시 잃는다. 지금 라이터는 업로드 후보를
 * 인메모리 {@code List<Path> closedFiles} 로만 들고 있어서, 죽고 나면
 * <b>무엇을 올려야 할지 알 수 없다.</b> 그리고 {@code S3ObjectStore.putTree} 는
 * 정규 파일이면 전부 올리므로 <b>쓰는 중인 파일까지 올린다</b> —
 * 푸터 없는 객체가 cold 에 들어가면 그 파티션 조회 전체가 실패한다.
 *
 * <p><b>불변식 셋</b>
 * <ol>
 *   <li>{@code ready/} 의 모든 파일은 푸터가 있다 ({@code ParquetWriter.close()} 를 통과했다)</li>
 *   <li>{@code inflight/} 의 모든 파일은 폐기 가능하다 — 그 데이터의 오프셋은 아직 커밋되지
 *       않았으므로 재생으로 복구된다. <b>{@link OffsetLedger} 의 커밋 규칙과 한 세트다.</b>
 *       커밋 규칙이 틀리면 폐기가 곧 유실이 된다</li>
 *   <li>업로드 성공 즉시 로컬 파일을 지운다. {@code ready/} 에 있다는 사실 자체가 "아직 안 올라감"이다</li>
 * </ol>
 *
 * <p>두 디렉터리를 <b>형제로</b> 두는 것이 중요하다. {@code ready/} 아래에 임시 디렉터리를 파면
 * 업로드 스캔에 걸리고, 다른 파일시스템에 두면 {@code ATOMIC_MOVE} 가 성립하지 않는다.
 */
public final class LocalSpool {

    private static final String INFLIGHT = "inflight";
    private static final String READY = "ready";

    private final Path root;

    public LocalSpool(Path root) throws IOException {
        this.root = root;
        Files.createDirectories(root.resolve(INFLIGHT));
        Files.createDirectories(root.resolve(READY));
    }

    /** 쓰는 중인 파일의 경로. 라이터는 여기에 쓴다. */
    public Path inflight(String relativePath) {
        return root.resolve(INFLIGHT).resolve(relativePath);
    }

    public Path readyRoot() {
        return root.resolve(READY);
    }

    /**
     * 닫힌 파일을 업로드 후보로 승격한다.
     *
     * <p>{@code ATOMIC_MOVE} 라 "반쯤 보이는" 상태가 없다 — {@code ready/} 에 이름이 보이면
     * 그 파일은 완결된 것이다. 다만 이름의 원자성만 보장하고 데이터를 flush 하지는 않는다.
     * 위협 모델(프로세스 죽음까지 방어, 커널 패닉은 방어하지 않음)은 ADR-0006 에 적었다.
     *
     * @return {@code ready/} 아래의 최종 경로
     */
    public Path promote(Path inflightFile) throws IOException {
        Path relative = root.resolve(INFLIGHT).relativize(inflightFile);
        Path target = readyRoot().resolve(relative);
        Files.createDirectories(target.getParent());

        try {
            Files.move(inflightFile, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            // 같은 파일시스템이 아니면 원자성이 깨진다 — 복사 중간 상태가 업로드 후보로 보인다.
            // 조용히 폴백하지 않고 실패시킨다. 그게 이 클래스가 존재하는 이유이기 때문이다.
            throw new IOException("inflight 와 ready 가 다른 파일시스템에 있다. "
                    + "같은 파일시스템의 형제 디렉터리여야 한다: " + root, e);
        }
        return target;
    }

    /**
     * 재시작 복구. <b>{@code inflight/} 를 전부 지우고 {@code ready/} 를 업로드 큐로 되살린다.</b>
     *
     * @return 업로드해야 할 파일들. 경로 순이라 실행마다 순서가 같다
     */
    public List<Path> recover() throws IOException {
        deleteRecursively(root.resolve(INFLIGHT));
        Files.createDirectories(root.resolve(INFLIGHT));

        try (Stream<Path> s = Files.walk(readyRoot())) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".parquet"))
                    .sorted()
                    .toList();
        }
    }

    /** 업로드가 끝난 파일. 지워야 "ready 에 있다 = 아직 안 올라감"이 성립한다. */
    public void released(Path readyFile) throws IOException {
        Files.deleteIfExists(readyFile);
        pruneEmptyParents(readyFile.getParent());
    }

    /**
     * {@code ready/} 의 모든 파일이 실제로 읽을 수 있는지 확인한다.
     *
     * <p>ADR-0001 때문에 Java 는 Parquet 레코드를 읽을 수 없지만 <b>푸터는 읽을 수 있다.</b>
     * 불변식 1을 검증할 수 있는 유일한 수단이다.
     *
     * @return 푸터가 깨진 파일들. 비어 있어야 정상
     */
    public List<Path> verifyReady() throws IOException {
        List<Path> broken = new ArrayList<>();
        for (Path p : recoverWithoutCleaning()) {
            try {
                ParquetStats.readFooter(new org.apache.parquet.io.LocalInputFile(p));
            } catch (IOException | RuntimeException e) {
                broken.add(p);
            }
        }
        return broken;
    }

    private List<Path> recoverWithoutCleaning() throws IOException {
        try (Stream<Path> s = Files.walk(readyRoot())) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".parquet"))
                    .sorted()
                    .toList();
        }
    }

    /** 빈 파티션 디렉터리가 쌓이면 업로드 스캔이 느려진다. */
    private void pruneEmptyParents(Path dir) throws IOException {
        Path stop = readyRoot();
        Path current = dir;
        while (current != null && !current.equals(stop) && current.startsWith(stop)) {
            try (Stream<Path> entries = Files.list(current)) {
                if (entries.findAny().isPresent()) return;
            }
            Files.deleteIfExists(current);
            current = current.getParent();
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
