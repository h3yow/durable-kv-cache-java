package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.exception.PersistenceException;
import com.github.h3yow.durablekvcachejava.model.WalRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * WAL 顺序追加写入器：每条记录编码为一行并追加 '\n'。
 *
 * <p>持久性保证：{@code fsyncEachWrite=true} 时，{@link #append(WalRecord)} 在
 * 文件内容强制刷盘（{@link FileChannel#force(boolean)} force(true)，含元数据）之后才返回；
 * 进程异常终止只会让“最后一次未完成写入”缺失或产生无 '\n' 的残行，
 * 恢复端将其视为未提交尾部直接忽略，绝不会读到半条记录。
 *
 * <p>文件命名：{@code wal-<起始seq,19位>-<创建时间>.log}；
 * 同一时刻只有一个活动 WAL，checkpoint 负责切换并归档旧 WAL。
 */
public class WalWriter {

    private final Path file;
    private final boolean fsyncEachWrite;
    private FileChannel channel;
    private long bytesWritten;
    private boolean closed;

    public WalWriter(Path file, boolean fsyncEachWrite) {
        this.file = file;
        this.fsyncEachWrite = fsyncEachWrite;
        this.closed = true;
    }

    /** 打开（创建父目录；文件已存在则以追加模式打开，不截断已有内容）。 */
    public synchronized void open() {
        if (!closed) {
            return;
        }
        try {
            Files.createDirectories(file.getParent());
            this.channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            this.bytesWritten = channel.size();
            this.closed = false;
        } catch (IOException e) {
            throw new PersistenceException("cannot open WAL " + file, e);
        }
    }

    /**
     * 追加一条记录；开启 fsync 时写完并强制刷盘后才返回，调用方即可视为已确认持久化。
     */
    public synchronized void append(WalRecord record) {
        if (closed) {
            throw new PersistenceException("WAL is closed: " + file, null);
        }
        byte[] payload = (record.encode() + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try {
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            if (fsyncEachWrite) {
                channel.force(true);
            }
            bytesWritten += payload.length;
        } catch (IOException e) {
            throw new PersistenceException("append failed for key=" + record.key()
                    + " seq=" + record.sequence(), e);
        }
    }

    /** 强制把内容（及元数据）刷到物理介质。 */
    public synchronized void force() {
        if (closed) {
            return;
        }
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new PersistenceException("fsync failed for " + file, e);
        }
    }

    /** 当前活动 WAL 已写入字节数。 */
    public synchronized long bytesWritten() {
        return bytesWritten;
    }

    public Path file() {
        return file;
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    /** 关闭句柄（不删除文件）。 */
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            channel.force(true);
            channel.close();
        } catch (IOException e) {
            throw new PersistenceException("close failed for " + file, e);
        } finally {
            closed = true;
        }
    }

    /** 构造按约定命名的新 WAL 路径。 */
    public static Path newWalPath(Path dir, long startSequence, long epochMs) {
        return dir.resolve(String.format("wal-%019d-%d.log", Math.max(0L, startSequence), epochMs));
    }
}
