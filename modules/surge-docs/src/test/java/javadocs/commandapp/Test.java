// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp;

import javadocs.commandapp.account.BankAccount;
import javadocs.commandapp.command.BankAccountCommand;
import javadocs.commandapp.command.CreateAccount;
import javadocs.commandapp.command.DebitAccount;
import surge.javadsl.common.CommandResult;
import surge.javadsl.testkit.MockedSurgeEngine;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class Test {

    // #sample_test_1
    void testSendCommand() throws Exception {

        MockedSurgeEngine<UUID, BankAccount, BankAccountCommand, BankAccountCommand> mockedSurgeEngine;
        mockedSurgeEngine = new MockedSurgeEngine<>();

        UUID accountNumber = UUID.randomUUID();
        UUID otherAccountNumber = UUID.randomUUID();

        CreateAccount createAccount = new CreateAccount(accountNumber, "Jane", "Doe", 1000);
        DebitAccount debitAccount = new DebitAccount(otherAccountNumber, 50);
        BankAccount expectedBankAccount = new BankAccount(accountNumber, "Jane", "Doe", 1000);

        mockedSurgeEngine.whenSendCommand(accountNumber, createAccount).returnAggregate(expectedBankAccount);
        mockedSurgeEngine.whenSendCommand(otherAccountNumber, debitAccount).noSuchAggregate();

        CompletionStage<CommandResult<BankAccount>> actualResult;
        actualResult = mockedSurgeEngine.get()
                .aggregateFor(accountNumber).sendCommand(createAccount);

        // compare actual result to expected result
        // NOTE: must resolve/block the CompletionStage/CompletableFuture
    }
    // #sample_test_1

    // #sample_test_2
    void testGetAggregate() throws Exception {

        MockedSurgeEngine<UUID, BankAccount, BankAccountCommand, BankAccountCommand> mockedSurgeEngine;
        mockedSurgeEngine = new MockedSurgeEngine<>();

        UUID accountNumber = UUID.randomUUID();

        BankAccount fakeBankAccount = new BankAccount(accountNumber, "Jane", "Doe", 1000);

        mockedSurgeEngine.whenGetAggregate(accountNumber).returnAggregate(fakeBankAccount);

        CompletionStage<Optional<BankAccount>> result;
        result = mockedSurgeEngine.get()
                .aggregateFor(accountNumber).getState();

        // compare result to expected result
        // NOTE: must resolve/block the CompletionStage/CompletableFuture

    }
    // #sample_test_2



}
