package dev.vuis.bfapi.http;

import dev.vuis.bfapi.util.Responses;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.GONE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class BfApiError {
	public static final BfApiError CLOUD_DISCONNECTED = new BfApiError(SERVICE_UNAVAILABLE, "cloud_disconnected");
	public static final BfApiError ENDPOINT_NOT_FOUND = new BfApiError(NOT_FOUND, "endpoint_not_found");
	public static final BfApiError ENDPOINT_REMOVED = new BfApiError(GONE, "endpoint_removed");
	public static final BfApiError INTERNAL_ERROR = new BfApiError(INTERNAL_SERVER_ERROR, "internal_error");
	public static final BfApiError INVALID_INCLUDE_DETAILS = new BfApiError(BAD_REQUEST, "invalid_invalid_details");
	public static final BfApiError INVALID_INCLUDE_UUID = new BfApiError(BAD_REQUEST, "invalid_include_uuid");
	public static final BfApiError INVALID_METHOD = new BfApiError(METHOD_NOT_ALLOWED, "invalid_method");
	public static final BfApiError INVALID_UUID = new BfApiError(BAD_REQUEST, "invalid_uuid");
	public static final BfApiError INVALID_UUID_SET = new BfApiError(BAD_REQUEST, "invalid_uuid_set");
	public static final BfApiError INVALID_SECRET = new BfApiError(FORBIDDEN, "invalid_secret");
	public static final BfApiError INVALID_STUB = new BfApiError(BAD_REQUEST, "invalid_stub");
	public static final BfApiError MISSING_UUID = new BfApiError(BAD_REQUEST, "missing_uuid");
	public static final BfApiError PACKET_TIMEOUT = new BfApiError(GATEWAY_TIMEOUT, "packet_timeout");
	public static final BfApiError REFRESH_IN_PROGRESS = new BfApiError(CONFLICT, "refresh_in_progress");
	public static final BfApiError SERIALIZATION_ERROR = new BfApiError(INTERNAL_SERVER_ERROR, "serialization_error");
	public static final BfApiError SERVER_BUSY = new BfApiError(TOO_MANY_REQUESTS, "server_busy");
	public static final BfApiError UCD_UNAVAILABLE = new BfApiError(SERVICE_UNAVAILABLE, "ucd_unavailable");
	public static final BfApiError UUID_SET_TOO_LARGE = new BfApiError(BAD_REQUEST, "uuid_set_too_large");

	private final HttpResponseStatus status;
	private final String id;

	public FullHttpResponse response(ChannelHandlerContext ctx, FullHttpRequest msg) {
		return Responses.json(
			ctx, msg, status,
			w -> w.beginObject().name("error").value(id).endObject()
		);
	}
}
