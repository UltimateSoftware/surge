// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp;

import javadocs.commandapp.account.BankAccount;
import javadocs.commandapp.command.BankAccountCommand;
import javadocs.commandapp.command.CreateAccount;
import javadocs.commandapp.event.BankAccountEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import surge.javadsl.command.SurgeCommand;
import surge.javadsl.command.SurgeCommandBuilder;
import surge.javadsl.common.CommandFailure;
import surge.javadsl.common.CommandResult;
import surge.javadsl.common.CommandSuccess;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import surge.internal.utils.DiagnosticContextFuturePropagation;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // #bank_account_engine_class
        BankAccountSurgeModel bankAccountSurgeModel = new BankAccountSurgeModel();
        var ec = DiagnosticContextFuturePropagation.global();
        SurgeCommand<UUID, BankAccount, BankAccountCommand, BankAccountEvent> surgeCommand = new SurgeCommandBuilder()
                .withBusinessLogic(bankAccountSurgeModel, ec).build();
        surgeCommand.start();
        // #bank_account_engine_class

        // #sending_command_to_engine
        UUID accountNumber = UUID.randomUUID();
        logger.info("Account number is: {}", accountNumber);
        CreateAccount createAccount = new CreateAccount(accountNumber, "Jane Doe",
                "1234", 1000.0);

        CompletionStage<CommandResult<BankAccount>> completionStageCreateAccount = surgeCommand
                .aggregateFor(accountNumber).sendCommand(createAccount);

        completionStageCreateAccount.whenComplete((CommandResult<BankAccount> result, Throwable ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                if (result instanceof CommandSuccess) {
                    CommandSuccess<BankAccount> commandSuccess = (CommandSuccess<BankAccount>) result;
                    logger.info("Aggregate state is: {} ", commandSuccess.aggregateState());
                } else if (result instanceof CommandFailure) {
                    CommandFailure<BankAccount> commandFailure = (CommandFailure<BankAccount>) result;
                    commandFailure.reason().printStackTrace();
                }
            }
        });
        // #sending_command_to_engine

        // #getting_state_from_engine
        CompletionStage<Optional<BankAccount>> currentState = surgeCommand.aggregateFor(accountNumber).getState();
        currentState.whenComplete((bankAccount, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
            } else {
                logger.info("Current state is: {}", bankAccount);
            }
        });
        // #getting_state_from_engine
    }
}
