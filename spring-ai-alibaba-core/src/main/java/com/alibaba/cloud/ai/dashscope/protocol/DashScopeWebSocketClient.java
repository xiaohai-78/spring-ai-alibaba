/*
 * Copyright 2024-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.dashscope.protocol;

import com.alibaba.cloud.ai.dashscope.api.ApiUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Headers;
import okhttp3.Request.Builder;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.Dispatcher;
import okhttp3.Protocol;
import okhttp3.ConnectionPool;
import okhttp3.logging.HttpLoggingInterceptor;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SignalType;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kevinlin09
 */
public class DashScopeWebSocketClient extends WebSocketListener {

	private final Logger logger = LoggerFactory.getLogger(DashScopeWebSocketClient.class);

	private final DashScopeWebSocketClientOptions options;
	private final OkHttpClient sharedOkHttpClient;

	private WebSocket webSocketClient;

	private AtomicBoolean isOpen;

	FluxSink<ByteBuffer> emitter;

	FluxSink<ByteBuffer> binary_emitter;

	FluxSink<String> text_emitter;

	public DashScopeWebSocketClient(DashScopeWebSocketClientOptions options) {
		this.options = options;
		this.isOpen = new AtomicBoolean(false);

		HttpLoggingInterceptor logging = new HttpLoggingInterceptor();
		// Use the constant for level, assuming it might be configurable later
		logging.setLevel(HttpLoggingInterceptor.Level.valueOf(DashScopeWebSocketClient.Constants.DEFAULT_HTTP_LOGGING_LEVEL));
		Dispatcher dispatcher = new Dispatcher();
		dispatcher.setMaxRequests(DashScopeWebSocketClient.Constants.DEFAULT_MAXIMUM_ASYNC_REQUESTS);
		dispatcher.setMaxRequestsPerHost(DashScopeWebSocketClient.Constants.DEFAULT_MAXIMUM_ASYNC_REQUESTS_PER_HOST);

		this.sharedOkHttpClient = new OkHttpClient.Builder()
			.connectTimeout(DashScopeWebSocketClient.Constants.DEFAULT_CONNECT_TIMEOUT)
			.readTimeout(DashScopeWebSocketClient.Constants.DEFAULT_READ_TIMEOUT)
			.writeTimeout(DashScopeWebSocketClient.Constants.DEFAULT_WRITE_TIMEOUT)
			.addInterceptor(logging)
			.dispatcher(dispatcher)
			.protocols(Collections.singletonList(Protocol.HTTP_1_1))
			.connectionPool(new ConnectionPool(
				DashScopeWebSocketClient.Constants.DEFAULT_CONNECTION_POOL_SIZE,
				DashScopeWebSocketClient.Constants.DEFAULT_CONNECTION_IDLE_TIMEOUT.getSeconds(),
				TimeUnit.SECONDS
			))
			.build();
	}

	public Flux<ByteBuffer> streamBinaryOut(String text) {
		Flux<ByteBuffer> flux = Flux.<ByteBuffer>create(emitter -> {
			this.binary_emitter = emitter;
		}, FluxSink.OverflowStrategy.BUFFER)
		.doOnCancel(() -> {
			logger.info("streamBinaryOut cancelled by subscriber.");
			closeWebSocketIfNeeded(1001, "Stream cancelled by client");
		})
		.doFinally(signalType -> {
			// Close only on explicit completion or error from this flux,
			// as the WebSocket might be used by another stream or kept open.
			// This behavior might need adjustment based on whether one client instance handles one logical stream or multiple.
			// Assuming one logical stream per client usage for now.
			if (signalType == reactor.core.publisher.SignalType.ON_COMPLETE || signalType == reactor.core.publisher.SignalType.ON_ERROR) {
				 logger.info("streamBinaryOut terminated with signal: {}", signalType);
				 closeWebSocketIfNeeded(1000, "Stream terminated");
			}
		});

		sendText(text); // This might establish the WebSocket

		return flux;
	}

	public Flux<String> streamTextOut(Flux<ByteBuffer> binary) {
		Flux<String> flux = Flux.<String>create(emitter -> {
			this.text_emitter = emitter;
		}, FluxSink.OverflowStrategy.BUFFER)
		.doOnCancel(() -> {
			logger.info("streamTextOut cancelled by subscriber.");
			closeWebSocketIfNeeded(1001, "Stream cancelled by client");
		})
		.doFinally(signalType -> {
			if (signalType == reactor.core.publisher.SignalType.ON_COMPLETE || signalType == reactor.core.publisher.SignalType.ON_ERROR) {
				 logger.info("streamTextOut terminated with signal: {}", signalType);
				 closeWebSocketIfNeeded(1000, "Stream terminated");
			}
		});

		// Subscribe to binary input; this binary flux might also need similar doOnCancel/doFinally
		// if its lifecycle dictates WebSocket closure. For now, assuming it's managed elsewhere or
		// its termination is tied to this streamTextOut's termination.
		binary.subscribe(this::sendBinary);

		return flux;
	}

	public void sendText(String text) {
		if (!isOpen.get()) {
			establishWebSocketClient();
		}

		boolean success = webSocketClient.send(text);

		if (!success) {
			logger.error("send text failed");
		}
	}

	public void sendBinary(ByteBuffer binary) {
		if (!isOpen.get()) {
			establishWebSocketClient();
		}

		// Check if establishWebSocketClient() failed and isOpen is still false
		if (!isOpen.get()) {
			logger.error("WebSocket not open, cannot send binary data. Establish client failed previously or was closed.");
			// Optionally, throw an exception or notify binary_emitter about the error
			if (this.binary_emitter != null && !this.binary_emitter.isCancelled()) {
				this.binary_emitter.error(new IOException("WebSocket not open, cannot send binary data."));
			}
			return;
		}

		boolean success = webSocketClient.send(ByteString.of(binary));

		if (!success) {
			logger.error("send binary failed");
			// Optionally, notify binary_emitter about the error
			if (this.binary_emitter != null && !this.binary_emitter.isCancelled()) {
				this.binary_emitter.error(new IOException("Failed to send binary data over WebSocket."));
			}
		}
	}

	private void establishWebSocketClient() {
		// OkHttpClient creation logic is removed from here
		try {
			// Use the shared client
			webSocketClient = this.sharedOkHttpClient.newWebSocket(buildConnectionRequest(), this);
		}
		catch (Throwable ex) {
			logger.error("create websocket failed: msg={}", ex.getMessage());
			// Consider if isOpen should be explicitly set to false or if an error should be propagated differently
			// For now, existing behavior is maintained.
		}
	}

	private Request buildConnectionRequest() {
		Builder bd = new Request.Builder();
		bd.headers(
				Headers.of(ApiUtils.getMapContentHeaders(options.getApiKey(), false, options.getWorkSpaceId(), null)));
		return bd.url(options.getUrl()).build();
	}

	private String getRequestBody(Response response) {
		String responseBody = "";
		if (response != null && response.body() != null) {
			try {
				responseBody = response.body().string();
			}
			catch (IOException ex) {
				logger.error("get response body failed: {}", ex.getMessage());
			}
		}
		return responseBody;
	}

	@Override
	public void onOpen(WebSocket webSocket, Response response) {
		logger.info("receive ws event onOpen: handle={}, body={}", webSocket, getRequestBody(response));
		isOpen.set(true);
	}

	@Override
	public void onClosed(WebSocket webSocket, int code, String reason) {
		logger.info("receive ws event onClosed: handle={}, code={}, reason={}", webSocket, code, reason);
		isOpen.set(false);
		emittersComplete("closed");
	}

	@Override
	public void onClosing(WebSocket webSocket, int code, String reason) {
		logger.info("receive ws event onClosing: handle={}, code={}, reason={}", webSocket.toString(), code, reason);
		emittersComplete("closing");
	}

	@Override
	public void onFailure(WebSocket webSocket, Throwable t, Response response) {
		String failureMessage = String.format("msg=%s, cause=%s, body=%s", t.getMessage(), t.getCause(),
				getRequestBody(response));
		logger.error("receive ws event onFailure: handle={}, {}", webSocket, failureMessage);
		isOpen.set(false);
		emittersError("failure", new Exception(failureMessage, t));
	}

	@Override
	public void onMessage(WebSocket webSocket, String text) {
		logger.debug("receive ws event onMessage(text): handle={}, text={}", webSocket, text);

		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		try {
			EventMessage message = objectMapper.readValue(text, EventMessage.class);
			switch (message.header.event) {
				case TASK_STARTED:
					logger.info("task started: text={}", text);
					break;
				case TASK_FINISHED:
					logger.info("task finished: text={}", text);
					emittersComplete("finished");
					break;
				case TASK_FAILED:
					logger.error("task failed: text={}", text);
					emittersError("task failed", new Exception());
					break;
				case RESULT_GENERATED:
					if (this.text_emitter != null) {
						text_emitter.next(text);
					}
					break;
				default:
					logger.error("task error: text={}", text);
					emittersError("unsupported event", new Exception());
			}
		}
		catch (Exception e) {
			logger.error("parse message failed: text={}, msg={}", text, e.getMessage());
		}
	}

	@Override
	public void onMessage(WebSocket webSocket, ByteString bytes) {
		logger.debug("receive ws event onMessage(bytes): handle={}, size={}", webSocket, bytes.size());
		if (this.binary_emitter != null) {
			binary_emitter.next(bytes.asByteBuffer());
		}
	}

	private void emittersComplete(String event) {
		if (this.binary_emitter != null && !this.binary_emitter.isCancelled()) {
			logger.info("binary emitter handling: complete on {}", event);
			this.binary_emitter.complete();
		}
		if (this.text_emitter != null && !this.text_emitter.isCancelled()) {
			logger.info("text emitter handling: complete on {}", event);
			this.text_emitter.complete();
			logger.info("done");
		}
	}

	private void emittersError(String event, Throwable t) {
		if (this.binary_emitter != null && !this.binary_emitter.isCancelled()) {
			logger.info("binary emitter handling: error on {}", event);
			this.binary_emitter.error(t);
		}
		if (this.text_emitter != null && !this.text_emitter.isCancelled()) {
			logger.info("text emitter handling: error on {}", event);
			this.text_emitter.error(t);
		}
	}

	public void close(int code, String reason) {
		logger.info("Explicitly closing WebSocket client with code: {}, reason: {}", code, reason);
		closeWebSocketIfNeeded(code, reason);
	}

	private void closeWebSocketIfNeeded(int code, String reason) {
		if (this.webSocketClient != null && this.isOpen.get()) {
			logger.debug("Attempting to close WebSocket (if open) with code: {}, reason: {}", code, reason);
			try {
				boolean closed = this.webSocketClient.close(code, reason);
				if (!closed) {
					// According to OkHttp WebSocket.close documentation, it returns false
					// if the connection is already closed or closing.
					// It might also return false if the close message could not be queued.
					// Forcing a cancel if close returns false and it's still open might be too aggressive
					// as onClosed/onFailure should eventually trigger.
					// logger.warn("WebSocket.close() returned false, but was open. State may be inconsistent.");
				}
				// Do not set isOpen.set(false) here; let the onClosed/onClosing callbacks handle it
				// to maintain consistent state management.
			} catch (Exception e) {
				// This catch is for potential exceptions from webSocketClient.close() itself,
				// though it's not common for it to throw.
				logger.error("Exception while trying to close WebSocket: {}", e.getMessage(), e);
				// Fallback to cancel if close fails catastrophically, or ensure resources are cleaned up.
				// For now, just log. The onClosed/onFailure should still be the primary path for state change.
				 if (this.webSocketClient != null) {
					this.webSocketClient.cancel(); // As a last resort if close fails badly
				 }
				 isOpen.set(false); // If close itself threw, state is likely broken.
				 emittersError("close_exception", e); // Notify emitters
			}
		} else {
			logger.debug("WebSocket already closed or not initialized, no action needed for closeWebSocketIfNeeded.");
		}
	}

	public static class Constants {

		private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(120);

		private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(60);

		private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(300);

		private static final Duration DEFAULT_CONNECTION_IDLE_TIMEOUT = Duration.ofSeconds(300);

		private static final Integer DEFAULT_CONNECTION_POOL_SIZE = 32;

		private static final Integer DEFAULT_MAXIMUM_ASYNC_REQUESTS = 32;

		private static final Integer DEFAULT_MAXIMUM_ASYNC_REQUESTS_PER_HOST = 32;

		private static final String DEFAULT_HTTP_LOGGING_LEVEL = "NONE";

	}

	// @formatter:off
	public enum EventType {

		// receive
		@JsonProperty("task-started")
		TASK_STARTED("task-started"),

		@JsonProperty("result-generated")
		RESULT_GENERATED("result-generated"),

		@JsonProperty("task-finished")
		TASK_FINISHED("task-finished"),

		@JsonProperty("task-failed")
		TASK_FAILED("task-failed"),

		// send
		@JsonProperty("run-task")
		RUN_TASK("run-task"),

		@JsonProperty("continue-task")
		CONTINUE_TASK("continue-task"),

		@JsonProperty("finish-task")
		FINISH_TASK("finish-task");

		private final String value;

		private EventType(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	public record EventMessage(
		@JsonProperty("header") EventMessageHeader header,
		@JsonProperty("payload") EventMessagePayload payload
	) {
		public record EventMessageHeader (
			@JsonProperty("task_id") String taskId,
			@JsonProperty("event") EventType event,
			@JsonProperty("error_code") String code,
			@JsonProperty("error_message") String message
		){}
		public record EventMessagePayload(
			@JsonProperty("output") JsonNode output,
			@JsonProperty("usage")  JsonNode usage
		){}
	}
	// @formatter:on

}
