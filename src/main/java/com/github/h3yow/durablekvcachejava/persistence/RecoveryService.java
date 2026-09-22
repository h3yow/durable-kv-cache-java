package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.exception.RecoveryException;
import com.github.h3yow.durablekvcachejava.model.CacheEntry;
import com.github.h3yow.durablekvcachejava.model.RecordType;
import com.github.h3yow.durablekvcachejava.model.WalRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 启动恢复（纯读操作，不修改磁盘，因此天然可重复执行、幂等）：
 * <ol>
 *   <li>读取 {@code snapshot.db}（不存在则视为空）；损坏直接失败，绝不静默忽略；</li>
 *   <li>按文件名起始 sequence 排好序读取所有 {@code wal-*.log}；</li>
 *   <li>按“文件顺序 + 行序”遍历，sequence 严格单调；重复/重叠序号的记录视为重复提交跳过，
 *       同一序号内容不一致则按损坏失败；</li>
 *   <li>每个键只保留最大 sequence 的记录（确定顺序）；</li>
 *   <li>以恢复时刻的服务端时钟为统一依据丢弃已过期键；</li>
 *   <li>产出活动记录（按 sequence 升序）与 {@link RecoveryReport}。</li>
 * </ol>
 *
 * <p>崩溃残尾语义：进程异常终止时，最后一次未完成的写入可能产生“没有 '\n' 的最后一行”。
 * 该行无论能否解码都视为未提交而忽略（在 report.detail 中注明）；其余行任何校验失败都
 * 以可区分原因（CHECKSUM_MISMATCH / TRUNCATED_RECORD / UNSUPPORTED_FORMAT /
 * SNAPSHOT_CORRUPT / IO_ERROR）失败，而不是继续运行。
 */
public class RecoveryService {

    private static final Pattern WAL_NAME =
            Pattern.compile("wal-(\\d{19})-(\\d+)\\.log(\\.archived)?");

    private final Path dataDir;
    private final SnapshotStore snapshotStore;
    private final Clock clock;

    /** 可注入时钟（测试可控）；默认系统时钟。 */
    public interface Clock {
        long nowEpochMs();
    }

    public RecoveryService(Path dataDir, SnapshotStore snapshotStore) {
        this(dataDir, snapshotStore, System::currentTimeMillis);
    }

    public RecoveryService(Path dataDir, SnapshotStore snapshotStore, Clock clock) {
        this.dataDir = dataDir;
        this.snapshotStore = snapshotStore;
        this.clock = clock;
    }

    public record RecoveredState(List<WalRecord> liveRecords, long lastSequence, RecoveryReport report) {
    }

    public RecoveredState recover() {
        long now = clock.nowEpochMs();
        long snapshotRecords = 0;
        long walRecords = 0;
        long skippedDuplicates = 0;
        long recoveredExpired = 0;
        long danglingTail = 0;

        // sequence -> record：全局按提交顺序去重，保证重复恢复/重复提交不叠加
        Map<Long, WalRecord> bySequence = new HashMap<>();

        List<WalRecord> snap = snapshotStore.readSnapshot();
        if (snap != null) {
            snapshotRecords = snap.size();
            for (WalRecord r : snap) {
                putIfNewer(bySequence, r);
            }
        }

        List<Path> walFiles = listWalFiles();
        for (Path wal : walFiles) {
            List<WalRecord> parsed;
            long[] dangling = new long[1];
            try {
                parsed = readWal(wal, dangling);
            } catch (IOException e) {
                throw new RecoveryException("IO_ERROR", "cannot read WAL " + wal.getFileName(), e);
            }
            danglingTail += dangling[0];
            walRecords += parsed.size();
            for (WalRecord r : parsed) {
                if (bySequence.containsKey(r.sequence())) {
                    WalRecord existing = bySequence.get(r.sequence());
                    if (!sameLogicalRecord(existing, r)) {
                        throw new RecoveryException("TRUNCATED_RECORD",
                                "conflicting records with same sequence=" + r.sequence()
                                        + " in " + wal.getFileName());
                    }
                    skippedDuplicates++;
                    continue;
                }
                putIfNewer(bySequence, r);
            }
        }

        // 序号单调校验（所有来源合并后必须是一条确定序列）
        List<WalRecord> ordered = new ArrayList<>(bySequence.values());
        ordered.sort(Comparator.comparingLong(WalRecord::sequence));
        long expected = -1L;
        for (WalRecord r : ordered) {
            if (r.sequence() <= expected) {
                throw new RecoveryException("TRUNCATED_RECORD",
                        "non-monotonic sequence detected at seq=" + r.sequence());
            }
            expected = r.sequence();
        }

        // 每键折叠到最大 sequence 记录，并在恢复时点统一判定过期
        Map<String, WalRecord> latestByKey = new HashMap<>();
        for (WalRecord r : ordered) {
            WalRecord prev = latestByKey.get(r.key());
            if (prev == null || r.sequence() > prev.sequence()) {
                latestByKey.put(r.key(), r);
            }
        }

        List<WalRecord> live = new ArrayList<>();
        for (WalRecord r : latestByKey.values()) {
            if (r.type() == RecordType.DELETE) {
                continue;
            }
            if (r.expireAtEpochMs() != CacheEntry.NO_EXPIRY && now >= r.expireAtEpochMs()) {
                recoveredExpired++;
                continue;
            }
            live.add(r);
        }
        live.sort(Comparator.comparingLong(WalRecord::sequence));

        long lastSequence = ordered.isEmpty() ? -1L : ordered.get(ordered.size() - 1).sequence();
        long applied = ordered.size();

        String detail = String.format(
                "recover@%d snapshotRecords=%d walFiles=%d walRecords=%d applied=%d duplicates=%d "
                        + "expiredAtRecovery=%d live=%d lastSeq=%d danglingTailLines=%d",
                now, snapshotRecords, walFiles.size(), walRecords, applied, skippedDuplicates,
                recoveredExpired, live.size(), lastSequence, danglingTail);

        RecoveryReport report = new RecoveryReport(true, snapshotRecords, walRecords, applied,
                skippedDuplicates, live.size(), recoveredExpired, lastSequence, null, detail);
        return new RecoveredState(List.copyOf(live), lastSequence, report);
    }

    private static void putIfNewer(Map<Long, WalRecord> bySequence, WalRecord r) {
        WalRecord existing = bySequence.get(r.sequence());
        if (existing != null) {
            if (!sameLogicalRecord(existing, r)) {
                throw new RecoveryException("TRUNCATED_RECORD",
                        "conflicting records with same sequence=" + r.sequence());
            }
            return;
        }
        bySequence.put(r.sequence(), r);
    }

    private static boolean sameLogicalRecord(WalRecord a, WalRecord b) {
        return a.type() == b.type()
                && a.key().equals(b.key())
                && a.version() == b.version()
                && a.expireAtEpochMs() == b.expireAtEpochMs()
                && java.util.Objects.equals(a.value(), b.value());
    }

    /** 读取一个 WAL：已提交行（以 '\n' 结束）逐条解码；最后无换行残尾按未提交忽略。 */
    private List<WalRecord> readWal(Path wal, long[] danglingCounter) throws IOException {
        byte[] raw = Files.readAllBytes(wal);
        if (raw.length == 0) {
            return List.of();
        }
        String text = new String(raw, StandardCharsets.UTF_8);
        boolean endsWithNewline = text.charAt(text.length() - 1) == '\n';
        String[] lines = text.split("\n", -1);
        List<WalRecord> out = new ArrayList<>();
        long lineNumber = 0;
        for (String line : lines) {
            lineNumber++;
            if (line.isEmpty()) {
                // 末尾 split 产生的空串：正常换行结尾时最后一段为空，跳过即可
                continue;
            }
            boolean isLastLine = lineNumber == lines.length;
            if (isLastLine && !endsWithNewline) {
                // 崩溃残留：未以换行结束的最后一行，无论内容如何都视为未提交
                danglingCounter[0]++;
                continue;
            }
            out.add(WalRecord.decode(line, lineNumber));
        }
        return out;
    }

    private List<Path> listWalFiles() {
        if (!Files.isDirectory(dataDir)) {
            try {
                Files.createDirectories(dataDir);
            } catch (IOException e) {
                throw new RecoveryException("IO_ERROR", "cannot create data dir " + dataDir, e);
            }
            return List.of();
        }
        List<Path> wals = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dataDir)) {
            stream.forEach(p -> {
                String name = p.getFileName().toString();
                if (name.equals(SnapshotStore.SNAPSHOT_FILE) || name.equals(SnapshotStore.TMP_FILE)) {
                    return;
                }
                Matcher m = WAL_NAME.matcher(name);
                if (m.matches()) {
                    wals.add(p);
                    return;
                }
                if (Files.isRegularFile(p)) {
                    throw new RecoveryException("UNSUPPORTED_FORMAT",
                            "unrecognized file in data directory: " + name
                                    + " (expected snapshot.db or wal-<seq>-<ts>.log)");
                }
            });
        } catch (IOException e) {
            throw new RecoveryException("IO_ERROR", "cannot list data dir " + dataDir, e);
        }
        wals.sort(Comparator
                .comparing((Path p) -> walStartSeq(p.getFileName().toString()))
                .thenComparing(p -> walEpoch(p.getFileName().toString()))
                // 同一起始 seq（正常不会出现，防御性处理）：活动 WAL 排在归档之后
                .thenComparing(p -> p.getFileName().toString().endsWith(".archived") ? 0 : 1));
        return wals;
    }

    private static long walStartSeq(String name) {
        Matcher m = WAL_NAME.matcher(name);
        if (!m.matches()) {
            throw new RecoveryException("UNSUPPORTED_FORMAT", "bad WAL filename: " + name);
        }
        return Long.parseLong(m.group(1));
    }

    private static long walEpoch(String name) {
        Matcher m = WAL_NAME.matcher(name);
        if (!m.matches()) {
            throw new RecoveryException("UNSUPPORTED_FORMAT", "bad WAL filename: " + name);
        }
        return Long.parseLong(m.group(2));
    }
}
