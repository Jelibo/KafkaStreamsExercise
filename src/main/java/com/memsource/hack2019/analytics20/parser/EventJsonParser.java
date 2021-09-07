package com.memsource.hack2019.analytics20.parser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.memsource.hack2019.analytics20.domain.Event;
import com.memsource.hack2019.analytics20.domain.TaskData;

/**
 * Gson parser for CDC Event
 */
public class EventJsonParser {

    /**
     * @param value Json event
     * @param gson Gson instance
     * @return parsed {@link Event}
     */
    public static Event parseEvent(final String value, final Gson gson) {
        JsonElement eventJson = gson.fromJson(value, JsonElement.class);
        Event event = new Event();
        JsonElement op = eventJson.getAsJsonObject().getAsJsonPrimitive("op");
        JsonElement after = eventJson.getAsJsonObject().get("after");
        JsonElement patch = eventJson.getAsJsonObject().get("patch");
        if (op != null) {
            event.setOp(op.getAsString());
        }
        if (after != null && !after.isJsonNull()) {
            event.setAfter(gson.fromJson(after.getAsString(), TaskData.class));
        }
        if (patch != null && !patch.isJsonNull()) {
            event.setPatch(gson.fromJson(patch.getAsString(), TaskData.class));
        }
        return event;
    }

}
