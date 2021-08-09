package com.example.format;

import com.example.account.BankAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Option;
import surge.core.SurgeAggregateReadFormatting;

import java.io.IOException;


public class SurgeAggregateReadFormattingBankAccount implements SurgeAggregateReadFormatting<BankAccount> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Option<BankAccount> readState(byte[] bytes) {
        BankAccount bankAccount;
        try {
            bankAccount = objectMapper.readValue(bytes, BankAccount.class);
            return scala.Option.apply(bankAccount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
