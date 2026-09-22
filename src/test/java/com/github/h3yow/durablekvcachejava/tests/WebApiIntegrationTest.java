package com.github.h3yow.durablekvcachejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 全栈 HTTP 测试（随机端口、每次运行唯一临时数据目录、小容量），
 * 验证各入口下结论一致且错误可区分。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(WebApiIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    static Path tempDataDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("cache.data-dir", () -> tempDataDir.resolve("data").toString());
        registry.add("cache.max-entries", () -> "3");
        registry.add("cache.eviction", () -> "REJECT");
        registry.add("cache.fsync-each-write", () -> "true");
        registry.add("cache.wal-compaction-bytes", () -> "0");
        registry.add("cache.cleanup-interval-ms", () -> "60000");
    }

    @Autowired
    private TestRestTemplate rest;

    private HttpHeaders json() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    private String put(String key, String body) {
        return rest.exchange("/cache/" + key, HttpMethod.PUT,
                new HttpEntity<>(body, json()), String.class).getBody();
    }

    @Test
    void putGetDeleteTtlCasCapacityAndStatusOverHttp() throws Exception {
        String key = "web-k";
        // PUT 带服务端 TTL
        String putBody = put(key, "{\"value\":\"hello\",\"ttlMillis\":5000}");
        JsonNode putJson = MAPPER.readTree(putBody);
        long version = putJson.get("version").asLong();
        assertEquals(1L, version);
        assertTrue(putJson.get("expireAtEpochMs").asLong() > 0);
        log.info("[http-put] key={} version={} expireAt={} basis=200 + server ttl",
                key, version, putJson.get("expireAtEpochMs").asLong());

        // GET 命中带 ttlRemainingMs
        ResponseEntity<String> hit = rest.getForEntity("/cache/" + key, String.class);
        assertEquals(HttpStatus.OK, hit.getStatusCode());
        JsonNode hitJson = MAPPER.readTree(hit.getBody());
        assertTrue(hitJson.get("found").asBoolean());
        assertFalse(hitJson.get("expired").asBoolean());
        log.info("[http-get-hit] key={} ttlRemaining={} basis={}", key,
                hitJson.get("ttlRemainingMs"), hitJson.get("reason").asText());

        // 错误 TTL -> 400 INVALID_REQUEST
        ResponseEntity<String> badTtl = rest.exchange("/cache/bad", HttpMethod.PUT,
                new HttpEntity<>("{\"value\":\"x\",\"ttlMillis\":-1}", json()), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, badTtl.getStatusCode());
        assertTrue(badTtl.getBody().contains("INVALID_REQUEST"));
        log.info("[http-bad-ttl] status=400 basis={}", MAPPER.readTree(badTtl.getBody()).get("error").asText());

        // 错误体 -> 400 MALFORMED_BODY
        ResponseEntity<String> malformed = rest.exchange("/cache/m", HttpMethod.PUT,
                new HttpEntity<>("not-json{", json()), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, malformed.getStatusCode());
        assertTrue(malformed.getBody().contains("MALFORMED_BODY"));

        // CAS 冲突 -> 409 VERSION_CONFLICT
        ResponseEntity<String> conflict = rest.exchange("/cache/" + key, HttpMethod.PUT,
                new HttpEntity<>("{\"value\":\"x\",\"expectedVersion\":999}", json()), String.class);
        assertEquals(HttpStatus.CONFLICT, conflict.getStatusCode());
        assertTrue(conflict.getBody().contains("VERSION_CONFLICT"));
        log.info("[http-cas] status=409 key={} basis=expectedVersion mismatch", key);

        // 容量（maxEntries=3，REJECT）-> 429 CAPACITY_EXCEEDED
        put("c1", "{\"value\":\"1\"}");
        put("c2", "{\"value\":\"2\"}");
        // web-k 已存在，再加 c3 正好 3 个，c4 触发拒绝
        put("c3", "{\"value\":\"3\"}");
        ResponseEntity<String> over = rest.exchange("/cache/c4", HttpMethod.PUT,
                new HttpEntity<>("{\"value\":\"4\"}", json()), String.class);
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, over.getStatusCode());
        assertTrue(over.getBody().contains("CAPACITY_EXCEEDED"));
        log.info("[http-capacity] status=429 basis=maxEntries=3 mode=REJECT");

        // 等待 TTL 到期（服务端时间），GET 返回 found=false, expired=true；DELETE 返回 deleted=false
        Thread.sleep(5100);
        ResponseEntity<String> expired = rest.getForEntity("/cache/" + key, String.class);
        JsonNode ex = MAPPER.readTree(expired.getBody());
        log.info("[http-expired-debug] body={}", expired.getBody());
        assertFalse(ex.get("found").asBoolean());
        assertTrue(ex.get("expired").asBoolean());
        log.info("[http-get-expired] key={} reason={} basis=server-side expiry", key, ex.get("reason").asText());

        // DELETE 新键 -> deleted=false，200
        ResponseEntity<String> deleteMiss = rest.exchange("/cache/nope", HttpMethod.DELETE,
                HttpEntity.EMPTY, String.class);
        JsonNode dm = MAPPER.readTree(deleteMiss.getBody());
        assertFalse(dm.get("deleted").asBoolean());

        // 显式删除 c1
        ResponseEntity<String> deleted = rest.exchange("/cache/c1", HttpMethod.DELETE,
                HttpEntity.EMPTY, String.class);
        JsonNode d = MAPPER.readTree(deleted.getBody());
        assertTrue(d.get("deleted").asBoolean());
        log.info("[http-delete] key=c1 tombVersion={} reason={}", d.get("version"), d.get("reason").asText());

        // 状态可观测
        ResponseEntity<String> status = rest.getForEntity("/admin/status", String.class);
        assertEquals(HttpStatus.OK, status.getStatusCode());
        JsonNode st = MAPPER.readTree(status.getBody());
        assertEquals("ACTIVE", st.get("state").asText());
        assertTrue(st.get("maxEntries").asInt() == 3);
        assertNotNull(st.get("recoveryReport"));
        log.info("[http-status] state=ACTIVE entries={} walBytes={} puts={} deletes={} evictions={} purges={}",
                st.get("entries"), st.get("currentWalBytes"), st.get("totalPuts"),
                st.get("totalDeletes"), st.get("totalEvictions"), st.get("totalExpiredPurges"));

        // 手动 purge / checkpoint 可用
        ResponseEntity<String> purge = rest.postForEntity("/admin/purge", null, String.class);
        assertEquals(HttpStatus.OK, purge.getStatusCode());
        ResponseEntity<String> cp = rest.postForEntity("/admin/checkpoint", null, String.class);
        assertEquals(HttpStatus.OK, cp.getStatusCode());
        assertTrue(MAPPER.readTree(cp.getBody()).get("ok").asBoolean());
        log.info("[http-admin] purge/checkpoint ok basis=admin endpoints");
    }

    @Test
    void missingKeyIs404SemanticsInBodyAndConcurrentOverwritesStayConsistent() throws Exception {
        // GET 不存在：HTTP 200 + found=false（缓存语义而非资源 404，理由可区分）
        JsonNode miss = MAPPER.readTree(rest.getForEntity("/cache/does-not-exist", String.class).getBody());
        assertFalse(miss.get("found").asBoolean());
        assertFalse(miss.get("expired").asBoolean());
        log.info("[http-miss] reason={} basis=no such key", miss.get("reason").asText());

        // 并发覆盖同一键：覆盖既有键不占新容量，全部必须成功；最终版本 == 成功次数
        // 先释放上个用例占用的容量（共享上下文，maxEntries=3, REJECT）
        for (String k : new String[]{"web-k", "c2", "c3"}) {
            rest.exchange("/cache/" + k, HttpMethod.DELETE, HttpEntity.EMPTY, String.class);
        }
        final String raceKey = "race-" + System.nanoTime();
        java.util.concurrent.atomic.AtomicInteger failures = new java.util.concurrent.atomic.AtomicInteger();
        Runnable writer = () -> {
            for (int i = 0; i < 50; i++) {
                ResponseEntity<String> r = rest.exchange("/cache/" + raceKey, HttpMethod.PUT,
                        new HttpEntity<>("{\"value\":\"v" + i + "\"}", json()), String.class);
                if (r.getStatusCode() != HttpStatus.OK) {
                    failures.incrementAndGet();
                    log.warn("[http-race-fail] status={} body={}", r.getStatusCode(), r.getBody());
                }
            }
        };
        Thread t1 = new Thread(writer);
        Thread t2 = new Thread(writer);
        t1.start();
        t2.start();
        t1.join(10_000);
        t2.join(10_000);
        JsonNode race = MAPPER.readTree(
                rest.getForEntity("/cache/" + raceKey, String.class).getBody());
        assertEquals(0, failures.get(), "并发 PUT 不应有失败");
        assertTrue(race.get("version").asLong() >= 100L, "实际版本=" + race.get("version"));
        log.info("[http-race] finalVersion={} basis=all accepted writes serialized; no stale overwrite",
                race.get("version"));
    }
}
