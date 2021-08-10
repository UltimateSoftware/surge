package com.example.account;

import com.example.command.BankAccountCommand;

import java.util.UUID;


public record DebitAccount(UUID accountNumber, double amount) implements BankAccountCommand {

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}
