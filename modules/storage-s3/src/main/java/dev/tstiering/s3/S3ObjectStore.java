package dev.tstiering.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * cold 계층 객체 저장소. Parquet 파일을 올리고, 올리는 데 걸린 시간을 되돌려준다.
 *
 * <p><b>multipart 를 SDK 의 {@code S3TransferManager} 대신 직접 구현한 이유</b>는 두 가지다.
 * 하나는 파트 단위 시간을 직접 재야 W3 의 "파일 크기별 업로드 시간" 표가 나오기 때문이고,
 * 다른 하나는 TransferManager 의 성능 경로가 {@code aws-crt-client} 를 요구하는데
 * 그러면 플랫폼별 네이티브 라이브러리가 배포물에 딸려오기 때문이다
 * (ADR-0001 에서 Arrow 를 기각한 것과 같은 이유).
 *
 * <p>파트 업로드는 <b>순차</b>다. 병렬화는 W3 측정에서 순차 수치가 로컬 S3 자체에 묶여
 * 있는지 확인한 뒤에 붙인다 — 지금 넣으면 무엇을 재고 있는지가 흐려진다.
 */
public final class S3ObjectStore implements Closeable {

    private final S3Client client;
    private final S3Settings settings;

    private S3ObjectStore(S3Client client, S3Settings settings) {
        this.client = client;
        this.settings = settings;
    }

    public static S3ObjectStore open(S3Settings settings) {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(settings.region()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(settings.pathStyleAccess())
                        .build());

        if (settings.endpoint() != null) {
            builder.endpointOverride(settings.endpoint());
        }
        if (settings.accessKey() != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(settings.accessKey(), settings.secretKey())));
        }

        return new S3ObjectStore(builder.build(), settings);
    }

    public S3Settings settings() {
        return settings;
    }

    // --- 버킷 ------------------------------------------------------------------

    /** 이미 있으면 아무것도 하지 않는다. 로컬 S3 를 매번 새로 띄우는 흐름을 위한 것. */
    public void createBucketIfAbsent() {
        try {
            client.headBucket(b -> b.bucket(settings.bucket()));
            return;
        } catch (NoSuchBucketException e) {
            // 아래에서 만든다
        } catch (S3Exception e) {
            if (e.statusCode() != 404) throw e;
        }

        client.createBucket(b -> {
            b.bucket(settings.bucket());
            // us-east-1 은 LocationConstraint 를 주면 오히려 거부한다.
            if (!"us-east-1".equals(settings.region())) {
                b.createBucketConfiguration(CreateBucketConfiguration.builder()
                        .locationConstraint(BucketLocationConstraint.fromValue(settings.region()))
                        .build());
            }
        });
    }

    // --- 업로드 ----------------------------------------------------------------

    /** 파일 크기에 따라 단일 PUT 과 multipart 중 하나를 고른다. */
    public UploadResult put(String key, Path file) throws IOException {
        return Files.size(file) > settings.multipartThreshold()
                ? putMultipart(key, file)
                : putSingle(key, file);
    }

    public UploadResult putSingle(String key, Path file) throws IOException {
        long bytes = Files.size(file);
        long startedAt = System.nanoTime();
        client.putObject(b -> b.bucket(settings.bucket()).key(key), RequestBody.fromFile(file));
        return new UploadResult(key, bytes, 1, false, elapsedSince(startedAt));
    }

    /**
     * 파트를 순차로 올린다. 중간에 실패하면 abort 해서 미완성 파트가 요금을 먹지 않게 한다
     * (실 AWS 에서 abort 를 빼먹으면 조용히 스토리지 비용이 쌓인다).
     */
    public UploadResult putMultipart(String key, Path file) throws IOException {
        long total = Files.size(file);
        int partSize = settings.partSize();
        long startedAt = System.nanoTime();

        String uploadId = client.createMultipartUpload(
                b -> b.bucket(settings.bucket()).key(key)).uploadId();

        try (RandomAccessFile in = new RandomAccessFile(file.toFile(), "r")) {
            List<CompletedPart> completed = new ArrayList<>();
            long offset = 0;
            int partNumber = 1;

            while (offset < total) {
                int len = (int) Math.min(partSize, total - offset);
                byte[] chunk = new byte[len];
                in.seek(offset);
                in.readFully(chunk);

                final int n = partNumber;
                String etag = client.uploadPart(
                        b -> b.bucket(settings.bucket()).key(key).uploadId(uploadId).partNumber(n),
                        RequestBody.fromBytes(chunk)).eTag();

                completed.add(CompletedPart.builder().partNumber(n).eTag(etag).build());
                offset += len;
                partNumber++;
            }

            client.completeMultipartUpload(b -> b
                    .bucket(settings.bucket())
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build()));

            return new UploadResult(key, total, completed.size(), true, elapsedSince(startedAt));
        } catch (RuntimeException | IOException e) {
            abortQuietly(key, uploadId, e);
            throw e;
        }
    }

    /**
     * 로컬 디렉터리 트리를 통째로 올린다. 파티션 디렉터리 구조가 그대로 객체 키가 된다.
     * 반환 목록의 순서는 파일 경로 순이라 실행마다 동일하다 — 결과표를 비교하려면 이게 필요하다.
     */
    public List<UploadResult> putTree(Path localRoot, String keyPrefix) throws IOException {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(localRoot)) {
            files = walk.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList();
        }

        String prefix = keyPrefix.isEmpty() || keyPrefix.endsWith("/") ? keyPrefix : keyPrefix + "/";
        List<UploadResult> results = new ArrayList<>(files.size());
        for (Path f : files) {
            // 객체 키는 항상 '/' 구분자다. Windows 경로 구분자가 섞이면 키가 깨진다.
            String rel = localRoot.relativize(f).toString().replace(java.io.File.separatorChar, '/');
            results.add(put(prefix + rel, f));
        }
        return results;
    }

    // --- 조회 ------------------------------------------------------------------

    public record ObjectSummary(String key, long size) {
    }

    /** 프리픽스 아래 전체 객체. 페이지네이션을 따라간다 (1000개에서 잘리면 파일 수 집계가 틀린다). */
    public List<ObjectSummary> list(String prefix) {
        List<ObjectSummary> out = new ArrayList<>();
        client.listObjectsV2Paginator(b -> b.bucket(settings.bucket()).prefix(prefix))
                .contents()
                .forEach(o -> out.add(new ObjectSummary(o.key(), o.size())));
        return out;
    }

    /**
     * 프리픽스 아래 전체를 지운다. <b>벤치 하네스용</b>이다 — 실행 사이에 프리픽스를 비우지 않으면
     * 이전 실행분이 다음 실행의 건수 대조에 섞여 <b>있지도 않은 중복으로 보인다</b>
     * (실제로 리밸런스 측정에서 한 번 그랬다).
     *
     * @return 지운 객체 수
     */
    public int deletePrefix(String prefix) {
        List<ObjectSummary> objects = list(prefix);
        // DeleteObjects 는 요청당 1000개가 상한이다.
        for (int from = 0; from < objects.size(); from += 1000) {
            var batch = objects.subList(from, Math.min(from + 1000, objects.size())).stream()
                    .map(o -> software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder()
                            .key(o.key()).build())
                    .toList();
            client.deleteObjects(b -> b.bucket(settings.bucket()).delete(d -> d.objects(batch)));
        }
        return objects.size();
    }

    // --- 보조 ------------------------------------------------------------------

    private void abortQuietly(String key, String uploadId, Throwable cause) {
        try {
            client.abortMultipartUpload(b -> b.bucket(settings.bucket()).key(key).uploadId(uploadId));
        } catch (RuntimeException abortFailure) {
            cause.addSuppressed(abortFailure);
        }
    }

    private static Duration elapsedSince(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }

    @Override
    public void close() {
        client.close();
    }
}
