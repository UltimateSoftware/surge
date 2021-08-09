package docs.example.account;

// #command_class
public record CreditAccount(UUID accountNumber, double amount) implements BankAccountCommand {


    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}
// #command_class


