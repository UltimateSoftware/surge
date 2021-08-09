package docs.example.account;



import java.util.UUID;
// #command_class
public record CreateAccount(UUID accountNumber, String accountOwner, String securityCode,
                            double initialBalance) implements BankAccountCommand {

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}
// #command_class


