// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import javadocs.commandapp.event.BankAccountEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import surge.core.SerializedMessage;
import surge.core.SurgeEventWriteFormatting;

// #surge_format
public class SurgeEventWriteFormattingBankEvent implements SurgeEventWriteFormatting<BankAccountEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedMessage writeEvent(BankAccountEvent evt) {
        try {
            return SerializedMessage.create(evt.getAccountNumber().toString(), objectMapper.writeValueAsBytes(evt));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
// #surge_format

