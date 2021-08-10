// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example;

import com.example.account.BankAccount;
import com.example.command.CreateAccount;
import com.example.command.CreditAccount;
import com.example.command.DebitAccount;
import com.example.event.BankAccountEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import surge.javadsl.command.SurgeCommand$;
import surge.javadsl.command.SurgeCommand;
import surge.javadsl.common.CommandFailure;
import surge.javadsl.common.CommandResult;
import surge.javadsl.common.CommandSuccess;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;


public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String AGGREGATE_STATE = "aggregate state is: {} ";

    public static void main(String[] args) {
        BankAccountSurgeModel bankAccountSurgeModel = new BankAccountSurgeModel();
        SurgeCommand<UUID, BankAccount, BankAccountCommand, ?, BankAccountEvent> surgeCommand =
                SurgeCommand$.MODULE$.create(bankAccountSurgeModel);
        surgeCommand.start();

        UUID accountNumber = UUID.randomUUID();
        logger.info("Account number is: {}", accountNumber);
        CreateAccount createAccount = new CreateAccount(accountNumber, "Jane Doe",
                "1234", 1000.0);

        CompletionStage<CommandResult<BankAccount>> completionStageCreateAccount =
                surgeCommand.aggregateFor(accountNumber).sendCommand(createAccount);

        completionStageCreateAccount.whenComplete((CommandResult<com.example.account.BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess<BankAccount> commandSuccess) {
                    logger.info(AGGREGATE_STATE, commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure<BankAccount> commandFailure) {
                    commandFailure.reason().printStackTrace();
                }
            }
        });

        CreditAccount creditAccount = new CreditAccount(accountNumber, 100.0);

        CompletionStage<CommandResult<BankAccount>> completionStageCreditAccount =
                surgeCommand.aggregateFor(accountNumber).sendCommand(creditAccount);
        completionStageCreditAccount.whenComplete((CommandResult<com.example.account.BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess<BankAccount> commandSuccess) {
                    logger.info(AGGREGATE_STATE, commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure<BankAccount> commandFailure) {
                    commandFailure.reason().printStackTrace();
                }
            }
        });

        DebitAccount debitAccount = new DebitAccount(accountNumber, 200.0);
        CompletionStage<CommandResult<BankAccount>> completionStageDebitAccount =
                surgeCommand.aggregateFor(accountNumber).sendCommand(debitAccount);
        completionStageDebitAccount.whenComplete((CommandResult<com.example.account.BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess<BankAccount> commandSuccess) {
                    logger.info(AGGREGATE_STATE, commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure<BankAccount> commandFailure) {
                    commandFailure.reason().printStackTrace();
                }
            }
        });

        CompletionStage<Optional<BankAccount>> currentState = surgeCommand.aggregateFor(accountNumber).getState();
        currentState.whenComplete((bankAccount, throwable) ->
        {
            if (throwable != null) {
                throwable.printStackTrace();
            } else {
                logger.info("current state of account is: {}", bankAccount);
            }
        });
    }
}
