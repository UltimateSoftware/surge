package com.example.command;

import com.example.account.BankAccount;
import com.example.BankAccountCreated;
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
                        createAccount.getAccountOwner()
                        , createAccount.getSecurityCode()
                        , createAccount.getInitialBalance());
                return Collections.singletonList(bankAccountCreated);

            }
        }
        if (command instanceof CreditAccount creditAccount) {
            if (aggregate.isPresent()) {
                BankAccount bankAccount = aggregate.get();
                BankAccountUpdated bankAccountUpdated = new BankAccountUpdated(bankAccount.accountId()
                        , bankAccount.balance() + creditAccount.getAmount());
                return Collections.singletonList(bankAccountUpdated);

            } else {
                throw new RuntimeException("Account does not exist");
            }
        }
        if (command instanceof DebitAccount debitAccount) {
            if (aggregate.isPresent()) {
                BankAccount bankAccount = aggregate.get();
                if (bankAccount.balance() >= debitAccount.getDebitAmount()) {
                    BankAccountUpdated bankAccountUpdated = new BankAccountUpdated(bankAccount.accountId(),
                            bankAccount.balance() - debitAccount.getDebitAmount());
                    return Collections.singletonList(bankAccountUpdated);

                } else {
                    throw new RuntimeException("InsufficientFund");
                }
            } else {
                throw new RuntimeException("Account does not exist");
            }
        }
        throw new RuntimeException("Invalid event command");
    }

    @Override
    public Optional<BankAccount> handleEvent(Optional<BankAccount> aggregate, BankAccountEvent event) {

        if(event instanceof BankAccountCreated bankAccountCreated){
            Optional<BankAccount> bankAccount;
             bankAccount = Optional.of(new BankAccount(event.getAccountNumber(), bankAccountCreated.getAccountOwner(),
                     bankAccountCreated.getSecurityCode(), bankAccountCreated.getBalance()));
            return  bankAccount;
        }
        if(event instanceof BankAccountUpdated bankAccountUpdated){
            return aggregate.map((item)-> new BankAccount(item.accountId(),item.accountOwner()
                    ,item.securityCode(),bankAccountUpdated.getBalance()));
        }
        throw new RuntimeException("Unhandled event");
    }

}
