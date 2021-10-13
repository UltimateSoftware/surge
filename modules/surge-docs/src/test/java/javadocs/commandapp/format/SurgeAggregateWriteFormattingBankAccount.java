// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import javadocs.commandapp.account.BankAccount;
import surge.core.SerializedAggregate;
import surge.core.SurgeAggregateWriteFormatting;
import surge.serialization.Serializer;

// #surge_format
public class SurgeAggregateWriteFormattingBankAccount implements SurgeAggregateWriteFormatting<BankAccount> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedAggregate writeState(BankAccount bankAccount) {
        try {
            byte[] bankAccountBytes = objectMapper.writeValueAsBytes(bankAccount);
            return SerializedAggregate.create(bankAccountBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Serializer<BankAccount> stateSerializer() {
        return bankAccount -> {
            try {
                return objectMapper.writeValueAsBytes(bankAccount);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
// #surge_format
