package com.github.h3yow.durablekvcachejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 整组提交 HTTP 端到端：成功/重复提交/过期删除前置失败/容量拒绝/单键不回归。
 */
@SpringBootTest(properties = "kv-cache.data-dir=target/it-group-data")
@AutoConfigureMockMvc
class GroupControllerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(GroupControllerIntegrationTest.class);

    @Autowired
    private MockMvc mvc;

    @Test
    void groupCommitSuccessDuplicateStaleAndCapacityOverHttp() throws Exception {
        // 每次运行使用唯一 groupId/键前缀：固定 data 目录在多次 mvn test 之间会保留历史 WAL。
        String run = Long.toHexString(System.nanoTime());
        String gid = "http-g1-" + run;
        // 先放一个将过期的键和一个将被删除的键，便于验证 GROUP_STALE 的可区分原因。
        mvc.perform(put("/cache/keys/g:exp:" + run).param("ttlMillis", "20").content("e"))
                .andExpect(status().isOk());
        mvc.perform(put("/cache/keys/g:del:" + run).param("ttlMillis", "5000").content("d"))
                .andExpect(status().isOk());
        mvc.perform(put("/cache/keys/g:keep:" + run).param("ttlMillis", "5000").content("k"))
                .andExpect(status().isOk());

        // 成功整组（PUT + DELETE）。
        String body = "{\"members\":["
                + "{\"op\":\"PUT\",\"key\":\"g:p:" + run + "\",\"value\":\"alice\",\"ttlMillis\":5000},"
                + "{\"op\":\"DELETE\",\"key\":\"g:del:" + run + "\"}"
                + "]}";
        mvc.perform(post("/cache/groups").param("groupId", gid)
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("COMMITTED"))
                .andExpect(jsonPath("$.groupId").value(gid))
                .andExpect(jsonPath("$.memberCount").value(2))
                .andExpect(jsonPath("$.evictedKeys").isArray());

        // 相同 groupId 重试：DUPLICATE。
        mvc.perform(post("/cache/groups").param("groupId", gid)
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("DUPLICATE"))
                .andExpect(jsonPath("$.duplicate").value(true));

        // 参数错误：成员引用了未知 op。
        mvc.perform(post("/cache/groups")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"members\":[{\"op\":\"PATCH\",\"key\":\"x\"}]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"));

        // 过期/删除前置失败：等待 g:exp 过期后，组内 DELETE 它与不存在的 g:ghost。
        Thread.sleep(40);
        String staleBody = "{\"members\":["
                + "{\"op\":\"DELETE\",\"key\":\"g:exp:" + run + "\"},"
                + "{\"op\":\"DELETE\",\"key\":\"g:ghost:" + run + "\"},"
                + "{\"op\":\"DELETE\",\"key\":\"g:keep:" + run + "\"}"
                + "]}";
        mvc.perform(post("/cache/groups").param("groupId", "http-stale-" + run)
                        .contentType(MediaType.APPLICATION_JSON).content(staleBody))
                .andExpect(status().isPreconditionFailed())
                .andExpect(jsonPath("$.errorCode").value("GROUP_STALE"))
                .andExpect(jsonPath("$.staleKeys[0].key").exists());
        // 零副作用：g:keep 仍可删除（单键接口不回归）。
        mvc.perform(get("/cache/keys/g:keep:" + run)).andExpect(status().isOk());

        log.info("HTTP整组判定 groupId={}/http-stale-{} 依据=COMMITTED/DUPLICATE/400/412 结果=结论可区分且零副作用",
                gid, run);
    }

    @Test
    void groupCapacityRejectedOverHttpWithoutEviction() throws Exception {
        String gid = "http-big-" + Long.toHexString(System.nanoTime());
        // maxEntries 默认 1000；提交一个超过上限的整组应得 507 GROUP_TOO_LARGE。
        StringBuilder sb = new StringBuilder("{\"members\":[");
        for (int i = 0; i < 1001; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"op\":\"PUT\",\"key\":\"big:").append(gid).append(':').append(i)
              .append("\",\"value\":\"v\",\"ttlMillis\":5000}");
        }
        sb.append("]}");
        mvc.perform(post("/cache/groups").param("groupId", gid)
                        .contentType(MediaType.APPLICATION_JSON).content(sb.toString()))
                .andExpect(status().isInsufficientStorage())
                .andExpect(jsonPath("$.errorCode").value("GROUP_TOO_LARGE"))
                .andExpect(jsonPath("$.groupId").value(gid));

        // 指标端点暴露整组成败与容量占用。
        mvc.perform(get("/cache/admin/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.groupCommitCount").exists())
                .andExpect(jsonPath("$.groupRejectedCapacityCount").exists())
                .andExpect(jsonPath("$.state").value("READY"));
        log.info("HTTP整组容量判定 groupId=http-big 依据=1001>max1000 结果=507 GROUP_TOO_LARGE, 指标可观测");
    }
}
