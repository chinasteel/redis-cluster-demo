package com.redis.cluster.demo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@SpringBootTest
class DemoApplicationTests {
    @Resource
    private RedisConnectionFactory redisConnectionFactory;
    @Resource
    private RedisTemplate<String, String> redisTemplate;

    @Test
    void contextLoads() {
        Set<String> keys = new HashSet<>();
        RedisClusterConnection redisClusterConnection = redisConnectionFactory.getClusterConnection();
        Iterable<RedisClusterNode> redisClusterNodes = redisClusterConnection.clusterGetNodes();
        redisClusterNodes.forEach(redisClusterNode -> {
            if (!redisClusterNode.isMaster()) {
                return;
            }
            ScanOptions scanOptions = ScanOptions.scanOptions().count(Integer.MAX_VALUE).match("*").build();
            try (Cursor<byte[]> cursor = redisClusterConnection.scan(redisClusterNode, scanOptions)){
                cursor.forEachRemaining(bytes -> keys.add(redisTemplate.getStringSerializer().deserialize(bytes)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        keys.forEach(System.out::println);
    }

}
