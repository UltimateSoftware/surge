package com.example;

import com.example.account.BankAccount;
import com.example.command.BankAccountCommand;
import com.example.command.CreateAccount;
import com.example.event.BankAccountEvent;
import surge.javadsl.command.SurgeCommand;
import surge.javadsl.command.SurgeCommand$;
import surge.javadsl.common.CommandFailure;
import surge.javadsl.common.CommandResult;
import surge.javadsl.common.CommandSuccess;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class Main {
    private static BankAccount BankAccount;

    public static void main(String[] args) throws Exception {

        BankAccountSurgeModel bankAccountSurgeModel = new BankAccountSurgeModel();

        SurgeCommand<UUID, BankAccount, BankAccountCommand, ?, BankAccountEvent> surgeCommand =
                SurgeCommand$.MODULE$.create(bankAccountSurgeModel);
        surgeCommand.start();

        UUID accountNumber = UUID.randomUUID();

        CreateAccount createAccount = new CreateAccount(accountNumber, "Jane Doe",
                "1234", 1000.0);

        CompletionStage<CommandResult<BankAccount>> completionStage =
                surgeCommand.aggregateFor(accountNumber).sendCommand(createAccount);

        completionStage.whenComplete((CommandResult<com.example.account.BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess commandSuccess) {
                    System.out.println(commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure commandFailure) {
                    commandFailure.reason().printStackTrace();
                }
            }
        });
    }
}