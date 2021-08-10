// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example.format;

import com.example.event.BankAccountEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.collection.immutable.HashMap$;
import surge.core.SerializedMessage;
import surge.core.SurgeEventWriteFormatting;

public class SurgeEventWriteFormattingBankEvent implements SurgeEventWriteFormatting<BankAccountEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedMessage writeEvent(BankAccountEvent evt) {
        try {
            String key = evt.getAccountNumber().toString();
            byte[] evtByte = objectMapper.writeValueAsBytes(evt);
            scala.collection.immutable.Map<String, String> map = HashMap$.MODULE$.empty();
            return SerializedMessage.apply(key, evtByte, map);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
