// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import javadocs.commandapp.event.BankAccountEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.collection.immutable.HashMap;
import surge.core.SerializedMessage;
import surge.core.SurgeEventWriteFormatting;
import surge.serialization.*;

// #surge_format
public class SurgeEventWriteFormattingBankEvent implements SurgeEventWriteFormatting<BankAccountEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedMessage writeEvent(BankAccountEvent evt) {
        try {
            String key = evt.getAccountNumber().toString();
            byte[] evtByte = eventSerializer().serialize(evt).bytes();
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
                return new BytesPlusHeaders(objectMapper.writeValueAsBytes(event), new HashMap<>());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };
    }
}
// #surge_format

