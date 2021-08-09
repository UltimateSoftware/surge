package com.example;

import com.example.account.BankAccount;
import com.example.account.CreateAccount;
import com.example.account.CreditAccount;
import com.example.account.DebitAccount;
import com.example.command.BankAccountCommand;
import com.example.event.BankAccountEvent;
import surge.javadsl.command.SurgeCommand$;
import surge.javadsl.command.SurgeCommand;
import surge.javadsl.common.CommandFailure;
import surge.javadsl.common.CommandResult;
import surge.javadsl.common.CommandSuccess;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class Main {
    private static BankAccount BankAccount;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        BankAccountSurgeModel bankAccountSurgeModel = new BankAccountSurgeModel();
        SurgeCommand<UUID, BankAccount, BankAccountCommand, ?, BankAccountEvent> surgeCommand =
                SurgeCommand$.MODULE$.create(bankAccountSurgeModel);
        surgeCommand.start();

        UUID accountNumber = UUID.randomUUID();
        System.out.println(accountNumber);
        CreateAccount createAccount = new CreateAccount(accountNumber, "Jane Doe",
                "1234", 1000.0);

        CompletionStage<CommandResult<BankAccount>> completionStageCreateAccount =
                surgeCommand.aggregateFor(accountNumber).sendCommand(createAccount);

        completionStageCreateAccount.whenComplete((CommandResult<com.example.account.BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess<BankAccount> commandSuccess) {
                    System.out.println("aggregate state " + commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure<BankAccount> commandFailure) {
                    commandFailure.reason().printStackTrace();
                }
            }
        });

        CreditAccount creditAccount = new CreditAccount(accountNumber,100.0);

        CompletionStage<CommandResult<BankAccount>> completionStageCreditAccount =
                surgeCommand.aggregateFor(accountNumber).sendCommand(creditAccount);
        completionStageCreditAccount.whenComplete((CommandResult<com.example.account.BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess<BankAccount> commandSuccess) {
                    System.out.println("aggregate state " + commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure<BankAccount> commandFailure) {
                    commandFailure.reason().printStackTrace();
                }
            }
        });

        DebitAccount debitAccount = new DebitAccount(accountNumber,200.0);
        CompletionStage<CommandResult<BankAccount>> completionStageDebitAccount =
                surgeCommand.aggregateFor(accountNumber).sendCommand(debitAccount);
        completionStageDebitAccount.whenComplete((CommandResult<com.example.account.BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess<BankAccount> commandSuccess) {
                    System.out.println("aggregate state " + commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure<BankAccount> commandFailure) {
                    commandFailure.reason().printStackTrace();
                }
            }
        });
    }
}