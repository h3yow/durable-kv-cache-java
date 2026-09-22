package com.github.h3yow.durablekvcachejava.model;

/** 服务生命周期状态。恢复期间为 RECOVERING，不对外暴露任何数据。 */
public enum LifecycleState {
    CREATED,
    RECOVERING,
    ACTIVE,
    FAILED
}
