// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.command;

import java.util.UUID;

// #command_class
public record CreditAccount(UUID accountNumber, double amount) implements BankAccountCommand {
    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}
// #command_class


