// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example.command;

import com.example.BankAccountCommand;

import java.util.UUID;


public record DebitAccount(UUID accountNumber, double amount) implements BankAccountCommand {

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}
