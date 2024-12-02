package dev.ai.alibaba.samples.executor.std.json;

import com.alibaba.cloud.ai.graph.serializer.plain_text.PlainTextStateSerializer;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import dev.ai.alibaba.samples.executor.AgentAction;
import dev.ai.alibaba.samples.executor.AgentExecutor;
import dev.ai.alibaba.samples.executor.AgentFinish;
import dev.ai.alibaba.samples.executor.AgentOutcome;
import dev.ai.alibaba.samples.executor.IntermediateStep;
import lombok.NonNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class JSONStateSerializer extends PlainTextStateSerializer<AgentExecutor.State> {

	final ObjectMapper objectMapper;

	public JSONStateSerializer() {
		this(new ObjectMapper());
	}

	public JSONStateSerializer(@NonNull ObjectMapper objectMapper) {
		super(AgentExecutor.State::new);
		this.objectMapper = objectMapper;
		this.objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

		var module = new SimpleModule();
		module.addDeserializer(AgentExecutor.State.class, new StateDeserializer());
		module.addDeserializer(AgentOutcome.class, new AgentOutcomeDeserializer());
		module.addDeserializer(AgentAction.class, new AgentActionDeserializer());
		module.addDeserializer(AgentFinish.class, new AgentFinishDeserializer());
		module.addDeserializer(IntermediateStep.class, new IntermediateStepDeserializer());

		objectMapper.registerModule(module);
	}

	@Override
	public String mimeType() {
		return "application/json";
	}

	@Override
	public void write(AgentExecutor.State object, ObjectOutput out) throws IOException {
		var json = objectMapper.writeValueAsString(object);
		out.writeUTF(json);
	}

	@Override
	public AgentExecutor.State read(ObjectInput in) throws IOException, ClassNotFoundException {
		var json = in.readUTF();
		return objectMapper.readValue(json, AgentExecutor.State.class);
	}

}
