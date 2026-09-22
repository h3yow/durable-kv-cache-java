package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.exception.RecoveryException;
import com.github.h3yow.durablekvcachejava.model.CacheEntry;
import com.github.h3yow.durablekvcachejava.model.RecordType;
import com.github.h3yow.durablekvcachejava.model.WalRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 记录编解码的正常、边界与失败路径：每个断言打印键/内容与判定依据。
 */
class WalRecordCodecTest {

    private static final Logger log = LoggerFactory.getLogger(WalRecordCodecTest.class);

    @Test
    void roundTripPutAndDeleteIncludingUnicodeAndNewlines() {
        WalRecord put = WalRecord.put(42L, "k-中\n文", 3L, 1_700_000_000_000L, "v\r\n\t\"引号\"");
        WalRecord decoded = WalRecord.decode(put.encode(), 1L);
        assertEquals(put, decoded);
        assertFalse(put.encode().contains("\n"), "编码必须为单行，载荷段 Base64 无换行");
        log.info("[codec-roundtrip] key={} seq={} version={} expireAt={} basis=encode/decode equals",
                put.key(), put.sequence(), put.version(), put.expireAtEpochMs());

        WalRecord del = WalRecord.delete(43L, "k", 4L, CacheEntry.NO_EXPIRY);
        assertEquals(RecordType.DELETE, WalRecord.decode(del.encode(), 2L).type());
        log.info("[codec-roundtrip] key={} type=DELETE seq={} basis=encode/decode equals",
                del.key(), del.sequence());
    }

    @Test
    void neverExpireSentinelSurvives() {
        WalRecord put = WalRecord.put(1L, "eternal", 1L, CacheEntry.NO_EXPIRY, "x");
        WalRecord decoded = WalRecord.decode(put.encode(), 1L);
        assertEquals(CacheEntry.NO_EXPIRY, decoded.expireAtEpochMs());
        log.info("[codec-sentinel] key={} expireAt={} basis=NO_EXPIRY preserved",
                decoded.key(), decoded.expireAtEpochMs());
    }

    @Test
    void bitFlipInValueFieldYieldsChecksumMismatch() {
        WalRecord put = WalRecord.put(7L, "k", 1L, 123L, "original-value");
        String line = put.encode();
        int idx = line.length() - 2;
        char flipped = line.charAt(idx) == 'A' ? 'B' : 'A';
        String corrupted = line.substring(0, idx) + flipped + line.substring(idx + 1);
        RecoveryException ex = assertThrows(RecoveryException.class,
                () -> WalRecord.decode(corrupted, 5L));
        assertEquals("CHECKSUM_MISMATCH", ex.reason());
        log.info("[codec-corrupt] key={} reason={} basis=crc32 mismatch at line=5",
                put.key(), ex.reason());
    }

    @Test
    void truncatedLineYieldsTruncatedRecord() {
        RecoveryException ex = assertThrows(RecoveryException.class,
                () -> WalRecord.decode("DKVC|1|123|abc", 9L));
        assertEquals("TRUNCATED_RECORD", ex.reason());
        log.info("[codec-truncated] reason={} basis=field count != 9", ex.reason());
    }

    @Test
    void badMagicAndBadVersionYieldsUnsupportedFormat() {
        RecoveryException magic = assertThrows(RecoveryException.class,
                () -> WalRecord.decode("XXXX|1|x|1|PUT|1|1|aw==|dg==", 1L));
        assertEquals("UNSUPPORTED_FORMAT", magic.reason());

        String line = WalRecord.put(1L, "k", 1L, 1L, "v").encode().replaceFirst("\\|1\\|", "|9|");
        RecoveryException version = assertThrows(RecoveryException.class,
                () -> WalRecord.decode(line, 2L));
        assertEquals("UNSUPPORTED_FORMAT", version.reason());
        log.info("[codec-format] reasons={},{} basis=magic/format version",
                magic.reason(), version.reason());
    }
}
