// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import javadocs.commandapp.event.BankAccountEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.collection.immutable.HashMap;
import surge.core.SurgeEventWriteFormatting;
import surge.serialization.*;

// #surge_format
public class SurgeEventWriteFormattingBankEvent implements SurgeEventWriteFormatting<BankAccountEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String key(BankAccountEvent evt) {
        return evt.getAccountNumber().toString();
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

