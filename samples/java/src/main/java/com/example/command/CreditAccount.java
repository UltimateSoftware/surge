package com.example.command;

import java.util.UUID;

public record CreditAccount(UUID accountNumber, double amount) implements BankAccountCommand {

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}
