package docs.example.format;

import com.example.account.BankAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Option;
import surge.core.SurgeAggregateReadFormatting;

import java.io.IOException;

// #surge_model_class
public class SurgeAggregateReadFormattingBankAccount implements SurgeAggregateReadFormatting<BankAccount> {
    @Override
    public Option<BankAccount> readState(byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        BankAccount bankAccount;
        try {
            bankAccount = objectMapper.readValue(bytes, BankAccount.class);
            return Option.apply(bankAccount);
        } catch (IOException e) {
            return Option.empty();
        }
    }

}
// #surge_model_class
