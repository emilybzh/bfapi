package dev.vuis.bfapi.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vuis.bfapi.util.cache.ExpiryHolder;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IdentifiableCacheHolderTest {
	@Test
	void failedRequesterCompletesPendingFutureAndAllowsRetry() {
		AtomicInteger requests = new AtomicInteger();
		IdentifiableCacheHolder<String, String> holder = new IdentifiableCacheHolder<>(
			_ -> {
				requests.incrementAndGet();
				throw new IllegalStateException("cloud disconnected");
			},
			Duration.ofSeconds(1)
		);

		CompletableFuture<ExpiryHolder<String>> first = holder.get("key");
		assertTrue(first.isCompletedExceptionally());
		assertEquals(1, requests.get());
		assertTrue(holder.pending.isEmpty());

		CompletableFuture<ExpiryHolder<String>> second = holder.get("key");
		assertTrue(second.isCompletedExceptionally());
		assertEquals(2, requests.get());
		assertTrue(holder.pending.isEmpty());
	}

	@Test
	void failedBulkRequesterClearsEveryPendingFuture() {
		AtomicInteger requests = new AtomicInteger();
		IdentifiableCacheHolder<String, String> holder = new IdentifiableCacheHolder<>(
			_ -> {
				requests.incrementAndGet();
				throw new IllegalStateException("cloud disconnected");
			},
			Duration.ofSeconds(1)
		);

		var futures = holder.get(Set.of("first", "second"));

		assertEquals(1, requests.get());
		assertTrue(futures.values().stream().allMatch(CompletableFuture::isCompletedExceptionally));
		assertTrue(holder.pending.isEmpty());
	}
}
