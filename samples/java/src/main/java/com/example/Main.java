package com.example;

import com.example.account.BankAccount;
import com.example.account.CreateAccount;
import com.example.command.BankAccountCommand;
import com.example.event.BankAccountEvent;
import surge.javadsl.command.SurgeCommand$;
import surge.javadsl.command.SurgeCommand;
import surge.javadsl.common.CommandResult;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class Main {
    public static void main(String[] args) {
        BankAccountSurgeModel bankAccountSurgeModel = new BankAccountSurgeModel();
        SurgeCommand<UUID, BankAccount, BankAccountCommand, ?, BankAccountEvent> surgeCommand =
                SurgeCommand$.MODULE$.create(bankAccountSurgeModel);

        CreateAccount createAccount = new CreateAccount(UUID.randomUUID(), "Jane Doe",
                "1234", 1000.0);
        CompletionStage<CommandResult<BankAccount>> completionStage =
                surgeCommand.aggregateFor(createAccount.getAccountNumber()).sendCommand(createAccount);

    }
}