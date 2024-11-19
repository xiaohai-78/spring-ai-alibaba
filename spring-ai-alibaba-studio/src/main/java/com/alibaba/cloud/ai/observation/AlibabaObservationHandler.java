package com.alibaba.cloud.ai.observation;

import com.alibaba.cloud.ai.entity.ModelObservationDetailEntity;
import com.alibaba.cloud.ai.entity.ModelObservationEntity;
import com.alibaba.cloud.ai.mapper.ModelObservationDetailMapper;
import com.alibaba.cloud.ai.mapper.ModelObservationMapper;
import io.micrometer.core.instrument.Clock;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.advisor.observation.AdvisorObservationContext;
import org.springframework.ai.chat.client.observation.ChatClientObservationContext;
import org.springframework.ai.chat.observation.ChatModelObservationContext;
import org.springframework.ai.embedding.observation.EmbeddingModelObservationContext;
import org.springframework.ai.vectorstore.observation.VectorStoreObservationContext;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * @Description: AlibabaObservationHandler
 * @Author: 肖云涛
 * @Date: 2024/11/17
 */
public class AlibabaObservationHandler implements ObservationHandler<Observation.Context> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlibabaObservationHandler.class);
    private final Clock clock;
    private final Tracer tracer;

    private final ModelObservationMapper modelObservationMapper;

    private final ModelObservationDetailMapper modelObservationDetailMapper;

    public AlibabaObservationHandler(ModelObservationMapper modelObservationMapper, ModelObservationDetailMapper modelObservationDetailMapper) {
        this.clock = Clock.SYSTEM;
        this.tracer = GlobalOpenTelemetry.getTracer("com.alibaba.cloud.ai");
        this.modelObservationMapper = modelObservationMapper;
        this.modelObservationDetailMapper = modelObservationDetailMapper;
    }

    @Override
    public void onStart(Observation.Context context) {
        long startTime = getCurrentTimeMillis();
        context.put("startTime", startTime);

        SpanBuilder spanBuilder = tracer.spanBuilder(context.getName())
                .setAttribute("component", "AlibabaChatClient")
                .setAttribute("start_time", startTime);
        Span span = spanBuilder.startSpan();

        context.put("span", span);
        LOGGER.info("onStart: Operation '{}' started. Start time: {}", context.getName(), startTime);
    }

    @Override
    public void onStop(Observation.Context context) {
        long startTime = context.getOrDefault("startTime", 0L);
        long endTime = getCurrentTimeMillis();
        long duration = endTime - startTime;

        Span span = context.getOrDefault("span", null);
        if (span != null) {
            span.setAttribute("duration_ns", duration);
            span.end();
        }

        if (context instanceof AdvisorObservationContext advisorObservationContext) {
            LOGGER.warn("Unknown Observation.Context type: {}, context: {}", context.getClass(), advisorObservationContext);
        }else if (context instanceof ChatModelObservationContext modelContext) {
            saveChatModelObservationContext(modelContext, duration);
        } else if(context instanceof ChatClientObservationContext chatClientObservationContext) {
            LOGGER.warn("Unknown Observation.Context type: {}, context: {}", context.getClass(), chatClientObservationContext);
        } else if(context instanceof EmbeddingModelObservationContext) {
            LOGGER.warn("Unknown Observation.Context type: {}", context.getClass());
        } else if(context instanceof VectorStoreObservationContext) {
            LOGGER.warn("Unknown Observation.Context type: {}", context.getClass());
        } else {
            LOGGER.warn("Unknown Observation.Context type: {}", context.getClass());
        }
    }

    @Override
    public void onError(Observation.Context context) {
        Span span = context.getOrDefault("span", null);
        if (span != null) {
            span.setAttribute("error", true);
            span.setAttribute("error.message", context.getError().getMessage());
            span.recordException(context.getError());
        }
        LOGGER.error("onError: Operation '{}' failed with error: {}", context.getName(), context.getError().getMessage());
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
        return true;
    }

    private long getCurrentTimeMillis() {
        return clock.monotonicTime() / 1_000_000;
    }

    /**
     * 保存 ChatModelObservationContext 的数据到数据库
     *
     * @param modelContext ChatModelObservationContext 实例
     * @param duration 操作耗时
     */
    private void saveChatModelObservationContext(ChatModelObservationContext modelContext, long duration) {

        long timestampInMillis = Instant.now().toEpochMilli();
        // 保存 ModelObservationEntity
        ModelObservationEntity modelObservationEntity = new ModelObservationEntity();
        modelObservationEntity.setName(modelContext.getName());
        modelObservationEntity.setAddTime(timestampInMillis);
        modelObservationEntity.setDuration(duration);
        modelObservationEntity.setModel(modelContext.getLowCardinalityKeyValue("gen_ai.response.model").getValue());
        modelObservationEntity.setTotalTokens(Math.toIntExact(modelContext.getResponse().getMetadata().getUsage().getTotalTokens()));
        modelObservationMapper.insert(modelObservationEntity);

        // 保存 ModelObservationDetailEntity
        ModelObservationDetailEntity modelObservationDetailEntity = new ModelObservationDetailEntity();
        modelObservationDetailEntity.setModelObservationId(modelObservationEntity.getId());
        modelObservationDetailEntity.setHighCardinalityKeyValues(modelContext.getHighCardinalityKeyValues().toString());
        modelObservationDetailEntity.setLowCardinalityKeyValues(modelContext.getLowCardinalityKeyValues().toString());
        modelObservationDetailEntity.setOperationMetadata(modelContext.getOperationMetadata().toString());
        modelObservationDetailEntity.setRequest(modelContext.getRequest().toString());
        modelObservationDetailEntity.setResponse(modelContext.getResponse().toString());
        modelObservationDetailEntity.setContextualName(modelContext.getContextualName());
        modelObservationDetailEntity.setAddTime(timestampInMillis);
        modelObservationDetailMapper.insert(modelObservationDetailEntity);
    }
}