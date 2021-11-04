// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.command;
import java.util.UUID;

// #command_class
public class CreateAccount implements BankAccountCommand {
    private final UUID accountNumber;
    private final String accountOwner;
    private double balance;
    private final String securityCode;

    public CreateAccount(UUID accountNumber, String accountOwner, String securityCode, double initialBalance) {
        this.accountNumber = accountNumber;
        this.accountOwner = accountOwner;
        this.securityCode = securityCode;
        this.balance = initialBalance;
    }

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }

    public String getAccountOwner() {
        return accountOwner;
    }

    public double getBalance() {
        return balance;
    }

    public String getSecurityCode() {
        return securityCode;
    }
}
// #command_class


