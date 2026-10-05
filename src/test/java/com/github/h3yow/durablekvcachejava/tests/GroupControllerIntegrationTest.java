package com.github.h3yow.durablekvcachejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 整组提交 HTTP 端到端：成功、幂等、前置失败 412、串组 409、容量 507、参数 400、组状态查询。
 * 数据目录由 {@link IsolatedTestDir} 在 Spring 上下文启动前分配：专属于本测试类、
 * 且每轮执行前清除上一轮残留，因此同一工作区反复执行（不 clean）结论一致——
 * 本轮的第一次提交永远判定为全新提交，不会被磁盘上的历史状态误判为重复。
 */
@SpringBootTest
@AutoConfigureMockMvc
class GroupControllerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(GroupControllerIntegrationTest.class);

    @DynamicPropertySource
    static void isolatedDataDir(DynamicPropertyRegistry registry) {
        registry.add("kv-cache.data-dir",
                () -> IsolatedTestDir.fresh("it-group-data").toString());
    }

    @Autowired
    private MockMvc mvc;

    private static final String COMMIT =
            """
            {"members":[
              {"key":"http:profile","action":"INSERT","value":"alice","ttlMillis":600000},
              {"key":"http:perms","action":"INSERT","value":"admin","ttlMillis":600000}
            ]}
            """;

    @Test
    void commitQueryDuplicateAndErrorsAreDistinguishableOverHttp() throws Exception {
        // 首次提交成功。
        mvc.perform(post("/cache/groups/http-grp-1/commit")
                        .contentType(MediaType.APPLICATION_JSON).content(COMMIT))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("OK"))
                .andExpect(jsonPath("$.groupId").value("http-grp-1"))
                .andExpect(jsonPath("$.duplicate").value(false))
                .andExpect(jsonPath("$.insertedKeys[0]").value("http:profile"))
                .andExpect(jsonPath("$.evictedKeys").isArray());

        // 单键读确认两键都生效。
        mvc.perform(get("/cache/keys/http:profile"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.value").value("alice"));

        // 相同 groupId + 相同成员重复提交 -> 幂等 OK_DUPLICATE。
        mvc.perform(post("/cache/groups/http-grp-1/commit")
                        .contentType(MediaType.APPLICATION_JSON).content(COMMIT))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("OK_DUPLICATE"))
                .andExpect(jsonPath("$.duplicate").value(true));

        // 组状态可查 / 未知组 404。
        mvc.perform(get("/cache/groups/http-grp-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.insertedKeys.length()").value(2));
        mvc.perform(get("/cache/groups/no-such-group"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value("NOT_FOUND"));

        // UPDATE 一个不存在的键 -> 412 GROUP_MEMBER_STALE，且 staleKeys 可见。
        String stale = "{\"members\":[{\"key\":\"missing\",\"action\":\"UPDATE\","
                + "\"value\":\"x\",\"ttlMillis\":1000}]}";
        mvc.perform(post("/cache/groups/http-grp-stale/commit")
                        .contentType(MediaType.APPLICATION_JSON).content(stale))
                .andExpect(status().isPreconditionFailed())
                .andExpect(jsonPath("$.errorCode").value("GROUP_MEMBER_STALE"))
                .andExpect(jsonPath("$.staleKeys[0]").value("missing"));

        // 相同 groupId 提交不同内容 -> 409 GROUP_ID_CONFLICT。
        String other = "{\"members\":[{\"key\":\"other\",\"action\":\"INSERT\","
                + "\"value\":\"x\",\"ttlMillis\":1000}]}";
        mvc.perform(post("/cache/groups/http-grp-1/commit")
                        .contentType(MediaType.APPLICATION_JSON).content(other))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("GROUP_ID_CONFLICT"));

        // 参数错误：空成员、缺 TTL、坏 action -> 400。
        mvc.perform(post("/cache/groups/http-grp-bad/commit")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"members\":[]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"));
        mvc.perform(post("/cache/groups/http-grp-bad2/commit")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"members\":[{\"key\":\"z\",\"action\":\"INSERT\",\"value\":\"x\"}]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"));
        mvc.perform(post("/cache/groups/http-grp-bad3/commit")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"members\":[{\"key\":\"z\",\"action\":\"BOGUS\"}]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"));

        log.info("HTTP整组判定 group=http-grp-1/stale/bad* 依据=提交200/重复OK_DUPLICATE/"
                + "前置412/串组409/参数400/组状态404 恢复结果=在线流程, 错误码全部可区分");
    }

    @Test
    void oversizedGroupReturns507WithReason() throws Exception {
        // 默认 max-entries=1000，构造 1001 个 INSERT 的整组 -> 507 CAPACITY_EXCEEDED + reason。
        StringBuilder sb = new StringBuilder("{\"members\":[");
        for (int i = 0; i < 1001; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"key\":\"big:").append(i)
              .append("\",\"action\":\"INSERT\",\"value\":\"v\",\"ttlMillis\":600000}");
        }
        sb.append("]}");
        mvc.perform(post("/cache/groups/http-grp-big/commit")
                        .contentType(MediaType.APPLICATION_JSON).content(sb.toString()))
                .andExpect(status().isInsufficientStorage())
                .andExpect(jsonPath("$.errorCode").value("CAPACITY_EXCEEDED"))
                .andExpect(jsonPath("$.reason").value("GROUP_TOO_LARGE"));
        log.info("HTTP整组容量判定 group=http-grp-big keys=big:0..big:1000 "
                + "依据=组写入1001>上限1000 恢复结果=507 CAPACITY_EXCEEDED reason=GROUP_TOO_LARGE");
    }
}
