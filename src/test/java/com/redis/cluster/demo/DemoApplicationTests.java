package com.redis.cluster.demo;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.AsyncTaskExecutor;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

@SpringBootTest
class DemoApplicationTests {
	private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplicationTests.class);
    @Resource
    private RedisConnectionFactory redisConnectionFactory;
    @Resource
    private RedisTemplate<String, String> redisTemplate;
	@Resource(name = "taskExecutor")
	private AsyncTaskExecutor asyncTaskExecutor;

    @Test
    void contextLoads() throws InterruptedException {
		CountDownLatch countDownLatch = new CountDownLatch(5000);
		for (int i = 0; i < 5000; i++) {
			asyncTaskExecutor.submit(() -> {
				Set<String> keys = new HashSet<>();
				RedisClusterConnection redisClusterConnection = redisConnectionFactory.getClusterConnection();
				Iterable<RedisClusterNode> redisClusterNodes = redisClusterConnection.clusterGetNodes();
				redisClusterNodes.forEach(redisClusterNode -> {
					if (!redisClusterNode.isMaster()) {
						return;
					}
					ScanOptions scanOptions = ScanOptions.scanOptions().count(Integer.MAX_VALUE).match("steel-*").build();
					try (Cursor<byte[]> cursor = redisClusterConnection.scan(redisClusterNode, scanOptions)) {
						cursor.forEachRemaining(bytes -> keys.add(redisTemplate.getStringSerializer().deserialize(bytes)));
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
				LOGGER.info("keys:  {}", keys);
				countDownLatch.countDown();
			});
		}
		countDownLatch.await();
    }

}
