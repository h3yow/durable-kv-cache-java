package com.github.h3yow.durablekvcachejava.cache;

/**
 * 整组提交中单个成员的动作类型。
 *
 * <ul>
 *   <li>{@link #INSERT}：要求键当前不存在（未过期），存在则整组前置失败；</li>
 *   <li>{@link #UPDATE}：要求键当前存活（存在且未过期），否则整组前置失败；</li>
 *   <li>{@link #DELETE}：要求键当前存活，整组失效时目标不存在/已过期同样整组前置失败。</li>
 * </ul>
 */
public enum GroupAction {
    INSERT, UPDATE, DELETE
}
