// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import javadocs.commandapp.account.BankAccount;
import scala.Option;
import surge.core.SurgeAggregateReadFormatting;
import surge.serialization.Deserializer;

import java.io.IOException;

// #surge_format
public class SurgeAggregateReadFormattingBankAccount implements SurgeAggregateReadFormatting<BankAccount> {
    @Override
    public Option<BankAccount> readState(byte[] bytes) {
        BankAccount bankAccount;
        try {
            bankAccount = stateDeserializer().deserialize(bytes);
            return Option.apply(bankAccount);
        } catch (RuntimeException e) {
            return Option.empty();
        }
    }

    @Override
    public Deserializer<BankAccount> stateDeserializer() {
            ObjectMapper objectMapper = new ObjectMapper();
            return body -> {
                try {
                    return objectMapper.readValue(body, BankAccount.class);
                } catch (IOException error) {
                    throw new RuntimeException(error);
                }
            };
    }
}
// #surge_format
