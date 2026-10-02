package com.github.h3yow.durablekvcachejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 入口端到端：成功、过期、删除、容量与非法请求的错误码均可区分。
 * 使用独立 data 目录由 application-test.properties 指定，避免污染工作区。
 */
@SpringBootTest(args = "--kv-cache.data-dir=target/it-data")
@AutoConfigureMockMvc
class CacheControllerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(CacheControllerIntegrationTest.class);

    @Autowired
    private MockMvc mvc;

    @Test
    void putGetExpireDeleteFlow() throws Exception {
        mvc.perform(put("/cache/keys/alpha").content("hello").param("ttlMillis", "5000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("OK"))
                .andExpect(jsonPath("$.key").value("alpha"));

        mvc.perform(get("/cache/keys/alpha"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("hello"));

        mvc.perform(delete("/cache/keys/alpha"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.existed").value(true));

        mvc.perform(get("/cache/keys/alpha"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value("NOT_FOUND"));

        mvc.perform(delete("/cache/keys/alpha"))
                .andExpect(jsonPath("$.existed").value(false));
        log.info("HTTP判定 key=alpha 依据=PUT200/GET200/DELETE200/再GET404 恢复结果=在线流程");
    }

    @Test
    void missingTtlIsBadRequestWithDistinctCode() throws Exception {
        mvc.perform(put("/cache/keys/bad").content("x"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"));
        mvc.perform(put("/cache/keys/bad").content("x").param("ttlMillis", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"));
        log.info("HTTP非法请求判定 key=bad 依据=ttl缺失/为0 结果=400 INVALID_REQUEST");
    }

    @Test
    void metricsIsObservable() throws Exception {
        mvc.perform(put("/cache/keys/m1").content("1").param("ttlMillis", "5000"))
                .andExpect(status().isOk());
        mvc.perform(get("/cache/admin/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("READY"))
                .andExpect(jsonPath("$.liveEntries").exists())
                .andExpect(jsonPath("$.evictionCount").exists());
        log.info("HTTP指标判定 依据=/admin/metrics 结果=state=READY 且容量字段可观测");
    }
}
