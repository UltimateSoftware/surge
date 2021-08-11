
// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example.format;

import com.example.account.BankAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Option;
import surge.core.SurgeAggregateReadFormatting;

import java.io.IOException;


public class SurgeAggregateReadFormattingBankAccount implements SurgeAggregateReadFormatting<BankAccount> {
    @Override
    public Option<BankAccount> readState(byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        BankAccount bankAccount;
        try {
            bankAccount = objectMapper.readValue(bytes, BankAccount.class);
            return scala.Option.apply(bankAccount);
        } catch (IOException e) {
            return scala.Option.empty();
        }
    }

}
