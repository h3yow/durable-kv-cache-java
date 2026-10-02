package com.github.h3yow.durablekvcachejava.web;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * 整组提交请求体：
 * <pre>
 * { "members": [ {"op":"PUT","key":"k","value":"v","ttlMillis":5000},
 *                {"op":"DELETE","key":"k2"} ] }
 * </pre>
 * groupId 通过查询参数 {@code ?groupId=...} 传入（可省略，省略时由服务端生成）。
 */
@JsonIgnoreProperties(ignoreUnknown = false)
public final class GroupCommitRequest {

    private List<Member> members;

    public List<Member> getMembers() {
        return members;
    }

    public void setMembers(List<Member> members) {
        this.members = members;
    }

    /** 单个成员操作。 */
    @JsonIgnoreProperties(ignoreUnknown = false)
    public static final class Member {
        private String op;
        private String key;
        private String value;
        private Long ttlMillis;

        public String getOp() {
            return op;
        }

        public void setOp(String op) {
            this.op = op;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Long getTtlMillis() {
            return ttlMillis;
        }

        public void setTtlMillis(Long ttlMillis) {
            this.ttlMillis = ttlMillis;
        }
    }
}
