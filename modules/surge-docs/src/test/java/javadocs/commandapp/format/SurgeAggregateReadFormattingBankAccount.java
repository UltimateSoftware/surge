// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import javadocs.commandapp.account.BankAccount;
import scala.Option;
import surge.core.SurgeAggregateReadFormatting;

import java.io.IOException;

// #surge_format
public class SurgeAggregateReadFormattingBankAccount implements SurgeAggregateReadFormatting<BankAccount> {
    @Override
    public Option<BankAccount> readState(byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        BankAccount bankAccount;
        try {
            bankAccount = objectMapper.readValue(bytes, BankAccount.class);
            return Option.apply(bankAccount);
        } catch (IOException e) {
            return Option.empty();
        }
    }

}
// #surge_format
