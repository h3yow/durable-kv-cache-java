package com.github.h3yow.durablekvcachejava.web.dto;

import java.util.List;

/**
 * 整组提交请求体（JSON）。
 *
 * <pre>
 * { "members": [ {"key":"session:1:profile","action":"UPDATE","value":"alice","ttlMillis":60000},
 *                {"key":"session:1:perms","action":"DELETE"} ] }
 * </pre>
 */
public final class GroupCommitRequest {

    private List<Member> members;

    public List<Member> getMembers() {
        return members;
    }

    public void setMembers(List<Member> members) {
        this.members = members;
    }

    /** 单个成员操作。 */
    public static final class Member {
        private String key;
        private String action; // INSERT / UPDATE / DELETE
        private String value;
        private Long ttlMillis;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
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
