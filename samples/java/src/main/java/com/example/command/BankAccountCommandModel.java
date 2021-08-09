package com.example.command;

import com.example.account.BankAccount;
import com.example.event.BankAccountCreated;
import com.example.event.BankAccountEvent;
import com.example.event.BankAccountUpdated;
import com.example.account.CreateAccount;
import com.example.account.CreditAccount;
import com.example.account.DebitAccount;
import surge.javadsl.command.AggregateCommandModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BankAccountCommandModel implements AggregateCommandModel<BankAccount, BankAccountCommand, BankAccountEvent> {
    @Override
    public List<BankAccountEvent> processCommand(Optional<BankAccount> aggregate, BankAccountCommand command) {

        if (command instanceof CreateAccount createAccount) {
            if (aggregate.isPresent()) {
                return new ArrayList<>();
            } else {
                BankAccountCreated bankAccountCreated = new BankAccountCreated(createAccount.getAccountNumber(),
                        createAccount.accountOwner()
                        , createAccount.securityCode()
                        , createAccount.initialBalance());
                return Collections.singletonList(bankAccountCreated);

            }
        }
        if (command instanceof CreditAccount creditAccount) {
            if (aggregate.isPresent()) {
                BankAccount bankAccount = aggregate.get();
                BankAccountUpdated bankAccountUpdated = new BankAccountUpdated(creditAccount.accountNumber()
                        , bankAccount.balance() + creditAccount.amount());
                return Collections.singletonList(bankAccountUpdated);

            } else {
                throw new RuntimeException("Account does not exist");
            }
        }
        if (command instanceof DebitAccount debitAccount) {
            if (aggregate.isPresent()) {
                BankAccount bankAccount = aggregate.get();
                if (bankAccount.balance() >= debitAccount.amount()) {
                    BankAccountUpdated bankAccountUpdated = new BankAccountUpdated(bankAccount.accountNumber(),
                            bankAccount.balance() - debitAccount.amount());
                    return Collections.singletonList(bankAccountUpdated);

                } else {
                    throw new RuntimeException("Insufficient funds");
                }
            } else {
                throw new RuntimeException("Account does not exist");
            }
        }
        throw new RuntimeException("Unhandled command");
    }

    @Override
    public Optional<BankAccount> handleEvent(Optional<BankAccount> aggregate, BankAccountEvent event) {

        if (event instanceof BankAccountCreated bankAccountCreated) {
            Optional<BankAccount> bankAccount;
            bankAccount = Optional.of(new BankAccount(event.getAccountNumber(), bankAccountCreated.accountOwner(),
                    bankAccountCreated.securityCode(), bankAccountCreated.balance()));
            return bankAccount;
        }
        if (event instanceof BankAccountUpdated bankAccountUpdated) {
            return aggregate.map((item) -> new BankAccount(item.accountNumber(), item.accountOwner()
                    , item.securityCode(), bankAccountUpdated.amount()));
        }
        throw new RuntimeException("Unhandled event");
    }

}
