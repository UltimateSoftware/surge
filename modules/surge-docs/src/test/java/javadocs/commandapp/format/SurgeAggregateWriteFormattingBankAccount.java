// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import javadocs.commandapp.account.BankAccount;
import scala.collection.immutable.HashMap$;
import surge.core.SerializedAggregate;
import surge.core.SurgeAggregateWriteFormatting;

// #surge_format
public class SurgeAggregateWriteFormattingBankAccount implements SurgeAggregateWriteFormatting<BankAccount> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedAggregate writeState(BankAccount bankAccount) {
        try {
            byte[] bankAccountBytes = objectMapper.writeValueAsBytes(bankAccount);
            scala.collection.immutable.Map<String, String> map = HashMap$.MODULE$.empty();
            return SerializedAggregate.apply(bankAccountBytes, map);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
// #surge_format
