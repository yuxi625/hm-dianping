package com.hmdp;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 批量为 tb_user 表中的用户生成登录 token，并写入 Redis。
 *
 * Redis 结构：
 *   key   : login:token:{token}          (String 类型的 key，Hash 类型的 value)
 *   value : Hash { id, nickName, icon? } icon 为空则不写入该 field
 *   ttl   : 180000 秒
 *
 * 同时把所有 token 输出到 tokens.txt，方便 JMeter 压测时用 CSV Data Set Config 读取。
 */
@Slf4j
@SpringBootTest
public class UserTokenTest {

    /** 与 RedisConstants.LOGIN_USER_KEY 保持一致 */
    private static final String LOGIN_TOKEN_KEY = "login:token:";
    /** ttl：180000 秒 */
    private static final long LOGIN_TOKEN_TTL = 180000L;
    /** 需要生成的用户数量 */
    private static final int USER_COUNT = 1000;
    /** token 输出文件，相对于项目根目录 */
    private static final String TOKEN_FILE = "tokens.txt";

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 方式一：逐条写入，逻辑最直观，1000 条大约几秒钟。
     */
    @Test
    void createTokens() throws IOException {
        // 1. 查询用户（按 id 升序取前 1000 个）
        List<User> users = userService.query()
                .orderByAsc("id")
                .last("limit " + USER_COUNT)
                .list();
        log.info("查询到 {} 个用户", users.size());

        List<String> tokens = new ArrayList<>(users.size());

        for (User user : users) {
            // 2. 用 hutool 的 UUID 生成 32 位无横线的 token
            String token = UUID.randomUUID().toString(true);
            tokens.add(token);

            // 3. 组装 hash：id、nickName 必存，icon 为空则不存
            Map<String, String> userMap = buildUserMap(user);

            // 4. 写入 Redis 并设置过期时间
            String key = LOGIN_TOKEN_KEY + token;
            stringRedisTemplate.opsForHash().putAll(key, userMap);
            stringRedisTemplate.expire(key, LOGIN_TOKEN_TTL, TimeUnit.SECONDS);
        }

        // 5. 导出 token 供 JMeter 使用
        writeTokensToFile(tokens);
        log.info("共生成 {} 个 token，已写入 Redis 和 {}", tokens.size(), TOKEN_FILE);
    }

    /**
     * 方式二：管道（pipeline）批量写入，只有一次网络往返，速度快很多。
     * 和方式一二选一执行即可。
     */
//    @Test
//    void createTokensByPipeline() throws IOException {
//        List<User> users = userService.query()
//                .orderByAsc("id")
//                .last("limit " + USER_COUNT)
//                .list();
//        log.info("查询到 {} 个用户", users.size());
//
//        List<String> tokens = new ArrayList<>(users.size());
//        for (int i = 0; i < users.size(); i++) {
//            tokens.add(UUID.randomUUID().toString(true));
//        }
//
//        stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
//            StringRedisConnection conn = (StringRedisConnection) connection;
//            for (int i = 0; i < users.size(); i++) {
//                String key = LOGIN_TOKEN_KEY + tokens.get(i);
//                conn.hMSet(key, buildUserMap(users.get(i)));
//                conn.expire(key, LOGIN_TOKEN_TTL);
//            }
//            return null;
//        });
//
//        writeTokensToFile(tokens);
//        log.info("共生成 {} 个 token，已写入 Redis 和 {}", tokens.size(), TOKEN_FILE);
//    }

    /**
     * 组装 hash 的 field-value。icon 为 null 或空串时不写该 field。
     */
    private Map<String, String> buildUserMap(User user) {
        Map<String, String> userMap = new HashMap<>(4);
        userMap.put("id", user.getId().toString());
        userMap.put("nickName", StrUtil.nullToEmpty(user.getNickName()));
        if (StrUtil.isNotBlank(user.getIcon())) {
            userMap.put("icon", user.getIcon());
        }
        return userMap;
    }

    /**
     * 每行一个 token，写入项目根目录下的 tokens.txt。
     */
    private void writeTokensToFile(List<String> tokens) throws IOException {
        Path path = Paths.get(TOKEN_FILE);
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            for (String token : tokens) {
                writer.write(token);
                writer.newLine();
            }
        }
        log.info("token 文件路径：{}", path.toAbsolutePath());
    }

    /**
     * 清理：删除所有 login:token:* 的 key（数据量大时慎用 keys，生产环境请用 scan）。
     */
    @Test
    void clearTokens() {
        java.util.Set<String> keys = stringRedisTemplate.keys(LOGIN_TOKEN_KEY + "*");
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
            log.info("已删除 {} 个 token", keys.size());
        }
    }
}