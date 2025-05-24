package com.alibaba.cloud.ai.dashscope.chat.cache;

import com.alibaba.cloud.ai.dashscope.api.DashScopeApi;
import com.alibaba.cloud.ai.dashscope.chat.DashScopeChatOptions;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Method;
import java.util.stream.Collectors;

@Component("promptCacheKeyGenerator") // Register as a Spring bean
public class PromptCacheKeyGenerator implements KeyGenerator {

    @Override
    public Object generate(Object target, Method method, Object... params) {
        if (params.length == 0 || !(params[0] instanceof Prompt)) {
            // Consider a more specific default key or an exception
            return "defaultEmptyPromptKey";
        }

        Prompt prompt = (Prompt) params[0];
        StringBuilder keyBuilder = new StringBuilder();

        // 1. Instructions
        if (!CollectionUtils.isEmpty(prompt.getInstructions())) {
            for (Message message : prompt.getInstructions()) {
                keyBuilder.append(message.getMessageType().name()).append(":");
                // Append message text, consider truncation or hashing for very long content
                keyBuilder.append(message.getText()).append(";#");
                if (message instanceof UserMessage && !CollectionUtils.isEmpty(((UserMessage) message).getMedia())) {
                    keyBuilder.append("MediaCount:").append(((UserMessage) message).getMedia().size()).append(";#");
                    ((UserMessage) message).getMedia().forEach(media ->
                        keyBuilder.append("MediaType:").append(media.getMimeType().toString()).append(";#")
                    );
                }
            }
        }

        // 2. Options
        if (prompt.getOptions() instanceof DashScopeChatOptions options) {
            keyBuilder.append("Model:").append(options.getModel()).append(";#");
            if (options.getTemperature() != null) {
                keyBuilder.append("Temp:").append(options.getTemperature()).append(";#");
            }
            if (options.getTopP() != null) {
                keyBuilder.append("TopP:").append(options.getTopP()).append(";#");
            }
            if (!CollectionUtils.isEmpty(options.getStop())) {
                 keyBuilder.append("Stop:").append(String.join(",", options.getStop())).append(";#");
            }
            if (!CollectionUtils.isEmpty(options.getTools())) {
                keyBuilder.append("Tools:").append(
                    options.getTools().stream()
                        .map(tool -> tool.function().name()) // Assuming tool.function() and name() are non-null
                        .collect(Collectors.joining(","))
                ).append(";#");
            }
            // Include other DashScopeChatOptions fields that influence the response significantly
            // For example: ResponseFormat, EnableSearch, etc.
             if (options.getResponseFormat() != null) {
                keyBuilder.append("ResponseFormat:").append(options.getResponseFormat()).append(";#");
            }
            if (options.getEnableSearch() != null) {
                keyBuilder.append("EnableSearch:").append(options.getEnableSearch()).append(";#");
            }
        }
        // For very long keys, consider hashing the final string (e.g., MD5 or SHA-256).
        return keyBuilder.toString();
    }
}
