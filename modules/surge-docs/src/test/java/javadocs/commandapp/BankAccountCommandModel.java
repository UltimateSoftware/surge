// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp;

import javadocs.commandapp.account.BankAccount;
import javadocs.commandapp.command.BankAccountCommand;
import javadocs.commandapp.command.CreateAccount;
import javadocs.commandapp.command.CreditAccount;
import javadocs.commandapp.command.DebitAccount;
import javadocs.commandapp.event.BankAccountCreated;
import javadocs.commandapp.event.BankAccountEvent;
import javadocs.commandapp.event.BankAccountUpdated;
import surge.javadsl.command.AggregateCommandModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

// #command_model_class
public class BankAccountCommandModel implements AggregateCommandModel<BankAccount, BankAccountCommand, BankAccountEvent> {

    @Override
    public List<BankAccountEvent> processCommand(Optional<BankAccount> aggregate, BankAccountCommand command) {

        // users can feel free to use vavr pattern matching as an alternative to instanceOf.
        if (command instanceof CreateAccount) {
            CreateAccount createAccount = (CreateAccount) command;
            if (aggregate.isPresent()) {
                return new ArrayList<>();
            } else {
                BankAccountCreated bankAccountCreated = new BankAccountCreated(createAccount.getAccountNumber(),
                        createAccount.getAccountOwner()
                        , createAccount.getSecurityCode()
                        , createAccount.getBalance());
                return Collections.singletonList(bankAccountCreated);

            }
        }
        if (command instanceof CreditAccount) {
            CreditAccount creditAccount = (CreditAccount) command;

            if (aggregate.isPresent()) {
                BankAccount bankAccount = aggregate.get();
                BankAccountUpdated bankAccountUpdated = new BankAccountUpdated(creditAccount.getAccountNumber()
                        , bankAccount.getBalance() + creditAccount.getAmount());
                return Collections.singletonList(bankAccountUpdated);

            } else {
                throw new RuntimeException("Account does not exist");
            }
        }
        if (command instanceof DebitAccount) {
            DebitAccount debitAccount = (DebitAccount) command;
            if (aggregate.isPresent()) {
                BankAccount bankAccount = aggregate.get();
                if (bankAccount.getBalance() >= debitAccount.getAmount()) {
                    BankAccountUpdated bankAccountUpdated = new BankAccountUpdated(bankAccount.getAccountNumber(),
                            bankAccount.getBalance() - debitAccount.getAmount());
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

        // users can feel free to use vavr pattern matching as an alternative to instanceOf.
        if (event instanceof BankAccountCreated) {
            BankAccountCreated bankAccountCreated = (BankAccountCreated)event;
            Optional<BankAccount> bankAccount;
            bankAccount = Optional.of(new BankAccount(event.getAccountNumber(), bankAccountCreated.getAccountOwner(),
                    bankAccountCreated.getSecurityCode(), bankAccountCreated.getBalance()));
            return bankAccount;
        }
        if (event instanceof BankAccountUpdated) {
            BankAccountUpdated bankAccountUpdated = (BankAccountUpdated)event;

            return aggregate.map((item) -> new BankAccount(item.getAccountNumber(), item.getAccountOwner()
                    , item.getSecurityCode(), bankAccountUpdated.getAmount()));
        }
        throw new RuntimeException("Unhandled event");
    }

}
// #command_model_class
