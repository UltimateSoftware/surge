package com.example;

import com.example.account.BankAccount;
import com.example.account.CreateAccount;
import com.example.account.CreditAccount;
import com.example.command.BankAccountCommand;
import com.example.event.BankAccountEvent;
import surge.javadsl.command.SurgeCommand$;
import surge.javadsl.command.SurgeCommand;
import surge.javadsl.common.CommandResult;
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
        UUID accountNumber = UUID.randomUUID();
        System.out.println(accountNumber);
        CreateAccount createAccount = new CreateAccount(accountNumber, "Jane Doe",
                "1234", 1000.0);
        CreditAccount creditAccount = new CreditAccount(accountNumber,100.0);

        CompletionStage<CommandResult<BankAccount>> completionStage =
                surgeCommand.aggregateFor(accountNumber).sendCommand(createAccount);
        CompletionStage<CommandResult<BankAccount>> completionStage1 =
                surgeCommand.aggregateFor(accountNumber).sendCommand(creditAccount);
//       CompletableFuture<CompletionStage<CommandResult<com.example.account.BankAccount>>> cf =
//               CompletableFuture.supplyAsync(()->surgeCommand.aggregateFor(accountNumber)
//                               .sendCommand(createAccount));
//       System.out.println(cf.get().toCompletableFuture().get());
    }
}