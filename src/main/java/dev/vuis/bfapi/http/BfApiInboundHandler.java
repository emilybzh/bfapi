package dev.vuis.bfapi.http;

import com.boehmod.bflib.cloud.common.AbstractClanData;
import com.boehmod.bflib.cloud.common.mm.report.MatchSummary;
import com.boehmod.bflib.cloud.common.player.status.PublicPlayerStatus;
import dev.vuis.bfapi.cloud.BfCloudData;
import dev.vuis.bfapi.cloud.BfConnection;
import dev.vuis.bfapi.cloud.BfPlayerData;
import dev.vuis.bfapi.cloud.BfPlayerInventory;
import dev.vuis.bfapi.cloud.unofficial.UnofficialCloudData;
import dev.vuis.bfapi.data.Serialization;
import dev.vuis.bfapi.util.Responses;
import dev.vuis.bfapi.util.Util;
import dev.vuis.bfapi.util.cache.ExpiryHolder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.booleans.BooleanObjectPair;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@ChannelHandler.Sharable
public final class BfApiInboundHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
	private static final int MAX_BULK_SIZE = 60;
	private static final int MAX_CONCURRENT_REQUESTS = 64;

	public final AtomicReference<BfConnection> connectionReference = new AtomicReference<>();
	public final AtomicReference<UnofficialCloudData> ucdReference = new AtomicReference<>();

	private final String ucdRefreshSecret;
	private final ExecutorService requestExecutor = Executors.newVirtualThreadPerTaskExecutor();
	private final Semaphore inFlight = new Semaphore(MAX_CONCURRENT_REQUESTS, true);

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
		boolean keepAlive = HttpUtil.isKeepAlive(msg);

		if (!inFlight.tryAcquire()) {
			FullHttpResponse response = BfApiError.SERVER_BUSY.response(ctx, msg);
			response.headers().set(HttpHeaderNames.RETRY_AFTER, "1");
			writeResponse(ctx, response, keepAlive);
			return;
		}

		// The HTTP event loop must never wait on the cloud connection. Retain the
		// request until the worker is done because SimpleChannelInboundHandler
		// releases it as soon as this method returns.
		msg.retain();
		try {
			requestExecutor.execute(() -> {
				try {
					handleRequest(ctx, msg, keepAlive);
				} catch (Throwable t) {
					log.error("unhandled error while handling {} {}", msg.method(), msg.uri(), t);
					writeResponse(ctx, BfApiError.INTERNAL_ERROR.response(ctx, msg), keepAlive);
				} finally {
					msg.release();
					inFlight.release();
				}
			});
		} catch (Throwable t) {
			msg.release();
			inFlight.release();
			log.error("failed to submit request {}", msg.uri(), t);
			writeResponse(ctx, BfApiError.SERVER_BUSY.response(ctx, msg), keepAlive);
		}
	}

	private void handleRequest(ChannelHandlerContext ctx, FullHttpRequest msg, boolean keepAlive) {
		QueryStringDecoder qs = new QueryStringDecoder(msg.uri());
		String path = qs.path();

		HttpResponse response = switch (path) {
			case "/api/v1/clan_data" -> clanData(ctx, msg, qs);
			case "/api/v1/clan_data/bulk" -> clanDataBulk(ctx, msg, qs);
			case "/api/v1/cloud_data" -> cloudData(ctx, msg);
			case "/api/v1/player_data" -> playerData(ctx, msg, qs);
			case "/api/v1/player_data/bulk" -> playerDataBulk(ctx, msg, qs);
			case "/api/v1/player_inventory" -> playerInventory(ctx, msg, qs, BfPlayerInventory.Target.ALL);
			case "/api/v1/player_inventory/equipped" -> playerInventory(ctx, msg, qs, BfPlayerInventory.Target.EQUIPPED);
			case "/api/v1/player_inventory/showcased" -> playerInventory(ctx, msg, qs, BfPlayerInventory.Target.SHOWCASED);
			case "/api/v1/player_matches" -> playerMatches(ctx, msg, qs);
//			case "/api/v1/player_status" -> playerStatus(ctx, msg, qs);
			case "/api/v1/player_status" -> BfApiError.ENDPOINT_REMOVED.response(ctx, msg);
//			case "/api/v1/player_status/bulk" -> playerStatusBulk(ctx, msg);
			case "/api/v1/player_status/bulk" -> BfApiError.ENDPOINT_REMOVED.response(ctx, msg);
			case "/api/v1/ucd/clan_list" -> ucdClanList(ctx, msg);
			case "/api/v1/ucd/player_exp_leaderboard" -> ucdPlayerExpLeaderboard(ctx, msg);
			case "/private/bf_ucd_refresh" -> bfUcdRefresh(ctx, msg);
			default -> null;
		};

		if (response == null) {
			response = BfApiError.ENDPOINT_NOT_FOUND.response(ctx, msg);
		}

		log.info("{} {} - {}", msg.method(), msg.uri(), response.status().code());
		writeResponse(ctx, response, keepAlive);
	}

	private static void writeResponse(ChannelHandlerContext ctx, HttpResponse response, boolean keepAlive) {
		if (keepAlive) {
			response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
			ctx.writeAndFlush(response);
		} else {
			ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
		}
	}

	private HttpResponse clanData(ChannelHandlerContext ctx, FullHttpRequest msg, QueryStringDecoder qs) {
		BfConnection connection = connectionReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		var stubResult = booleanFromParams(qs, "stub", BfApiError.INVALID_STUB);
		if (stubResult.right() != null) {
			return stubResult.right().response(ctx, msg);
		}
		boolean stub = stubResult.leftBoolean();

		Pair<UUID, BfApiError> uuidResult = uuidFromParams(qs);
		if (uuidResult.right() != null) {
			return uuidResult.right().response(ctx, msg);
		}
		UUID uuid = uuidResult.left();

		ExpiryHolder<AbstractClanData> data;
		try {
			data = connection.dataCache.clanData.get(uuid)
				.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("error while retrieving clan data", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}

		FullHttpResponse response = Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			w -> {
				AbstractClanData value = data.value();
				if (stub) {
					w.beginObject();
					Serialization.namedStub(w, value.getClanId(), value.getName());
					w.endObject();
				} else {
					Serialization.clan(w, value, connection.dataCache);
				}
			}
		);
		if (data.expires() != null) {
			Responses.cacheHeaders(response, data.expires());
		}
		return response;
	}

	private HttpResponse clanDataBulk(ChannelHandlerContext ctx, FullHttpRequest msg, QueryStringDecoder qs) {
		BfConnection connection = connectionReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.POST);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		var stubResult = booleanFromParams(qs, "stub", BfApiError.INVALID_STUB);
		if (stubResult.right() != null) {
			return stubResult.right().response(ctx, msg);
		}
		boolean stub = stubResult.leftBoolean();

		Pair<Set<UUID>, BfApiError> uuidsResult = parseUuidSet(msg.content());
		if (uuidsResult.right() != null) {
			return uuidsResult.right().response(ctx, msg);
		}

		var dataFutures = connection.dataCache.clanData.get(uuidsResult.left());
		try {
			CompletableFuture.allOf(dataFutures.values().toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException e) {
			log.error("error while retrieving bulk clan data", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}
		List<AbstractClanData> clanDatas = dataFutures.values().stream()
			.map(f -> f.join().value()).toList();

		return Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			w -> {
				w.beginArray();
				for (AbstractClanData clanData : clanDatas) {
					if (stub) {
						w.beginObject();
						Serialization.namedStub(w, clanData.getClanId(), clanData.getName());
						w.endObject();
					} else {
						Serialization.clan(w, clanData, connection.dataCache);
					}
				}
				w.endArray();
			}
		);
	}

	private HttpResponse cloudData(ChannelHandlerContext ctx, FullHttpRequest msg) {
		BfConnection connection = connectionReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		// we could use last modified headers but cloud stats change too often for it to really matter

		BfCloudData data;
		try {
			data = connection.dataCache.cloudStats.get()
				.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("error while retrieving cloud stats", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}

		return Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			w -> data.serialize(w, connection.dataCache)
		);
	}

	private HttpResponse playerData(ChannelHandlerContext ctx, FullHttpRequest msg, QueryStringDecoder qs) {
		BfConnection connection = connectionReference.get();
		UnofficialCloudData ucd = ucdReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		var stubResult = booleanFromParams(qs, "stub", BfApiError.INVALID_STUB);
		if (stubResult.right() != null) {
			return stubResult.right().response(ctx, msg);
		}
		boolean stub = stubResult.leftBoolean();

		Pair<UUID, BfApiError> uuidResult = uuidFromParams(qs);
		if (uuidResult.right() != null) {
			return uuidResult.right().response(ctx, msg);
		}
		UUID uuid = uuidResult.left();

		ExpiryHolder<BfPlayerData> data;
		try {
			data = connection.dataCache.playerData.get(uuid)
				.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("error while retrieving player data", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}

		FullHttpResponse response = Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			w -> {
				BfPlayerData value = data.value();
				if (stub) {
					w.beginObject();
					Serialization.namedStub(w, value.getUUID(), value.getUsername());
					w.endObject();
				} else {
					value.serialize(w, connection.dataCache, ucd);
				}
			}
		);
		if (data.expires() != null) {
			Responses.cacheHeaders(response, data.expires());
		}
		return response;
	}

	private HttpResponse playerDataBulk(ChannelHandlerContext ctx, FullHttpRequest msg, QueryStringDecoder qs) {
		BfConnection connection = connectionReference.get();
		UnofficialCloudData ucd = ucdReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.POST);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		var stubResult = booleanFromParams(qs, "stub", BfApiError.INVALID_STUB);
		if (stubResult.right() != null) {
			return stubResult.right().response(ctx, msg);
		}
		boolean stub = stubResult.leftBoolean();

		Pair<Set<UUID>, BfApiError> uuidsResult = parseUuidSet(msg.content());
		if (uuidsResult.right() != null) {
			return uuidsResult.right().response(ctx, msg);
		}

		var dataFutures = connection.dataCache.playerData.get(uuidsResult.left());
		try {
			CompletableFuture.allOf(dataFutures.values().toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException e) {
			log.error("error while retrieving bulk player data", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}
		List<BfPlayerData> playerDatas = dataFutures.values().stream()
			.map(f -> f.join().value()).toList();

		return Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			w -> {
				w.beginArray();
				for (BfPlayerData playerData : playerDatas) {
					if (stub) {
						w.beginObject();
						Serialization.namedStub(w, playerData.getUUID(), playerData.getUsername());
						w.endObject();
					} else {
						playerData.serialize(w, connection.dataCache, ucd);
					}
				}
				w.endArray();
			}
		);
	}

	private HttpResponse playerInventory(ChannelHandlerContext ctx, FullHttpRequest msg, QueryStringDecoder qs, BfPlayerInventory.Target target) {
		BfConnection connection = connectionReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		var includeUuidResult = booleanFromParams(qs, "include_uuid", BfApiError.INVALID_INCLUDE_UUID);
		if (includeUuidResult.right() != null) {
			return includeUuidResult.right().response(ctx, msg);
		}
		boolean includeUuid = includeUuidResult.leftBoolean();

		var includeDetailsResult = booleanFromParams(qs, "include_details", BfApiError.INVALID_INCLUDE_DETAILS);
		if (includeDetailsResult.right() != null) {
			return includeDetailsResult.right().response(ctx, msg);
		}
		boolean includeDetails = includeDetailsResult.leftBoolean();

		Pair<UUID, BfApiError> uuidResult = uuidFromParams(qs);
		if (uuidResult.right() != null) {
			return uuidResult.right().response(ctx, msg);
		}
		UUID uuid = uuidResult.left();

		ExpiryHolder<BfPlayerInventory> data;
		try {
			data = connection.dataCache.inventoryMinimal.get(uuid)
				.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("error while retrieving minimal player inventory", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}

		FullHttpResponse response = Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			w -> data.value().serialize(
				w, target, connection.registry, includeUuid, includeDetails,
				Util.unchecked(w2 -> {
					w2.name("player").beginObject();
					Serialization.playerStub(w2, connection.dataCache, uuid);
					w2.endObject();
				})
			)
		);
		if (data.expires() != null) {
			Responses.cacheHeaders(response, data.expires());
		}
		return response;
	}

	private HttpResponse playerMatches(ChannelHandlerContext ctx, FullHttpRequest msg, QueryStringDecoder qs) {
		BfConnection connection = connectionReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		Pair<UUID, BfApiError> uuidResult = uuidFromParams(qs);
		if (uuidResult.right() != null) {
			return uuidResult.right().response(ctx, msg);
		}
		UUID uuid = uuidResult.left();

		ExpiryHolder<List<MatchSummary>> data;
		try {
			data = connection.dataCache.playerMatches.get(uuid)
				.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("error while retrieving player status", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}

		FullHttpResponse response = Responses.json(
			ctx, msg,
			HttpResponseStatus.OK,
			w -> {
				w.beginObject();

				w.name("matches").beginArray();
				for (MatchSummary summary : data.value()) {
					Serialization.matchSummary(w, summary);
				}
				w.endArray();

				w.name("player").beginObject();
				Serialization.playerStub(w, connection.dataCache, uuid);
				w.endObject();

				w.endObject();
			}
		);
		if (data.expires() != null) {
			Responses.cacheHeaders(response, data.expires());
		}
		return response;
	}

	private HttpResponse playerStatus(ChannelHandlerContext ctx, FullHttpRequest msg, QueryStringDecoder qs) {
		BfConnection connection = connectionReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		Pair<UUID, BfApiError> uuidResult = uuidFromParams(qs);
		if (uuidResult.right() != null) {
			return uuidResult.right().response(ctx, msg);
		}
		UUID uuid = uuidResult.left();

		ExpiryHolder<PublicPlayerStatus> data;
		try {
			data = connection.dataCache.playerStatus.get(uuid)
				.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("error while retrieving player status", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}

		FullHttpResponse response = Responses.json(
			ctx, msg,
			HttpResponseStatus.OK,
			w -> Serialization.playerStatus(
				w, data.value(),
				Util.unchecked(w2 -> {
					w2.name("player").beginObject();
					Serialization.playerStub(w2, connection.dataCache, uuid);
					w2.endObject();
				})
			)
		);
		if (data.expires() != null) {
			Responses.cacheHeaders(response, data.expires());
		}
		return response;
	}

	private HttpResponse playerStatusBulk(ChannelHandlerContext ctx, FullHttpRequest msg) {
		BfConnection connection = connectionReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.POST);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		Pair<Set<UUID>, BfApiError> uuidsResult = parseUuidSet(msg.content());
		if (uuidsResult.right() != null) {
			return uuidsResult.right().response(ctx, msg);
		}

		var dataFutures = connection.dataCache.playerStatus.get(uuidsResult.left());
		try {
			CompletableFuture.allOf(dataFutures.values().toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException e) {
			log.error("error while retrieving bulk player status", e);
			return BfApiError.INTERNAL_ERROR.response(ctx, msg);
		} catch (TimeoutException e) {
			return BfApiError.PACKET_TIMEOUT.response(ctx, msg);
		}
		Map<UUID, PublicPlayerStatus> playerStatuses = dataFutures.entrySet().stream()
			.collect(Collectors.toMap(
				Map.Entry::getKey,
				e -> e.getValue().join().value()
			));

		return Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			w -> {
				w.beginArray();
				for (Map.Entry<UUID, PublicPlayerStatus> entry : playerStatuses.entrySet()) {
					Serialization.playerStatus(
						w, entry.getValue(),
						Util.unchecked(w2 -> {
							w2.name("player").beginObject();
							Serialization.playerStub(w2, connection.dataCache, entry.getKey());
							w2.endObject();
						})
					);
				}
				w.endArray();
			}
		);
	}

	private HttpResponse ucdClanList(ChannelHandlerContext ctx, FullHttpRequest msg) {
		UnofficialCloudData ucd = ucdReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (ucd == null) {
			return BfApiError.UCD_UNAVAILABLE.response(ctx, msg);
		}

		return Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			ucd::serializeClanList
		);
	}

	private HttpResponse ucdPlayerExpLeaderboard(ChannelHandlerContext ctx, FullHttpRequest msg) {
		UnofficialCloudData ucd = ucdReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.GET);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (ucd == null) {
			return BfApiError.UCD_UNAVAILABLE.response(ctx, msg);
		}

		Instant lastRefreshed = ucd.getLastRefreshed();

		if (lastRefreshed != null) {
			FullHttpResponse notModifiedResponse = Responses.checkIfModifiedSince(msg, lastRefreshed);
			if (notModifiedResponse != null) {
				return notModifiedResponse;
			}
		}

		FullHttpResponse response = Responses.json(
			ctx, msg, HttpResponseStatus.OK,
			ucd::serializePlayerLeaderboard
		);

		if (lastRefreshed != null) {
			Responses.lastModifiedHeaders(response, lastRefreshed);
		}

		return response;
	}

	private HttpResponse bfUcdRefresh(ChannelHandlerContext ctx, FullHttpRequest msg) {
		BfConnection connection = connectionReference.get();
		UnofficialCloudData ucd = ucdReference.get();

		FullHttpResponse methodResponse = Responses.checkMethod(ctx, msg, HttpMethod.POST);
		if (methodResponse != null) {
			return methodResponse;
		}
		if (connection == null || !connection.isConnectedAndVerified()) {
			return BfApiError.CLOUD_DISCONNECTED.response(ctx, msg);
		}

		ByteBuf content = msg.content();
		int contentLength = content.readableBytes();

		if (contentLength != ucdRefreshSecret.length()) {
			return BfApiError.INVALID_SECRET.response(ctx, msg);
		}

		byte[] secretBytes = new byte[contentLength];
		content.readBytes(secretBytes);
		String secret = new String(secretBytes, StandardCharsets.US_ASCII);

		if (!secret.equals(ucdRefreshSecret)) {
			return BfApiError.INVALID_SECRET.response(ctx, msg);
		}

		if (!ucd.startRefresh()) {
			return BfApiError.REFRESH_IN_PROGRESS.response(ctx, msg);
		}

		return new DefaultFullHttpResponse(
			msg.protocolVersion(),
			HttpResponseStatus.NO_CONTENT
		);
	}

	private static BooleanObjectPair<BfApiError> booleanFromParams(QueryStringDecoder qs, String key, BfApiError invalidError) {
		if (!qs.parameters().containsKey(key)) {
			return BooleanObjectPair.of(false, null);
		}
		try {
			return BooleanObjectPair.of(Boolean.parseBoolean(qs.parameters().get(key).getFirst()), null);
		} catch (Exception e) {
			return BooleanObjectPair.of(false, invalidError);
		}
	}

	private static Pair<UUID, BfApiError> uuidFromParams(QueryStringDecoder qs) {
		if (!qs.parameters().containsKey("uuid")) {
			return Pair.of(null, BfApiError.MISSING_UUID);
		}

		Optional<UUID> uuidParseResult = Util.parseUuidLenient(qs.parameters().get("uuid").getFirst());
		if (uuidParseResult.isEmpty()) {
			return Pair.of(null, BfApiError.INVALID_UUID);
		}
		return Pair.of(uuidParseResult.orElseThrow(), null);
	}

	private static Pair<Set<UUID>, BfApiError> parseUuidSet(ByteBuf content) {
		Set<Optional<UUID>> parsedUuids = Arrays.stream(content.toString(StandardCharsets.US_ASCII).split(","))
			.map(Util::parseUuidLenient).collect(Collectors.toSet());

		if (parsedUuids.stream().anyMatch(Optional::isEmpty)) {
			return Pair.of(null, BfApiError.INVALID_UUID_SET);
		}
		if (parsedUuids.size() > MAX_BULK_SIZE) {
			return Pair.of(null, BfApiError.UUID_SET_TOO_LARGE);
		}

		return Pair.of(parsedUuids.stream().map(Optional::orElseThrow).collect(Collectors.toSet()), null);
	}
}
