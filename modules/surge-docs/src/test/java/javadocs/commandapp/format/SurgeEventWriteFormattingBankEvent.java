// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import javadocs.commandapp.event.BankAccountEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import surge.core.SerializedMessage;
import surge.core.SurgeEventWriteFormatting;
import surge.serialization.Serializer;

// #surge_format
public class SurgeEventWriteFormattingBankEvent implements SurgeEventWriteFormatting<BankAccountEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedMessage writeEvent(BankAccountEvent evt) {
        try {
            String key = evt.getAccountNumber().toString();
            byte[] evtByte = eventSerializer().serialize(evt);
            return SerializedMessage.create(key, evtByte);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Serializer<BankAccountEvent> eventSerializer() {
        return event -> {
            try {
                return objectMapper.writeValueAsBytes(event);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };
    }
}
// #surge_format

