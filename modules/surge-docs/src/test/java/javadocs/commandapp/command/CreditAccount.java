// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.command;

import java.util.UUID;

// #command_class
public class CreditAccount implements BankAccountCommand {
    private final UUID accountNumber;
    private final double amount;
    public CreditAccount(UUID accountNumber, double amount) {
        this.accountNumber = accountNumber;
        this.amount = amount;
    }
    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }

    public double getAmount() {
        return amount;
    }
}
// #command_class


