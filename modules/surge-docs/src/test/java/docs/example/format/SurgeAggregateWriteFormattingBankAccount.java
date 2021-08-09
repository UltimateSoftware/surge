package docs.example.format;

import com.example.account.BankAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.collection.immutable.HashMap$;
import surge.core.SerializedAggregate;
import surge.core.SurgeAggregateWriteFormatting;

// #surge_model_class
public class SurgeAggregateWriteFormattingBankAccount implements SurgeAggregateWriteFormatting<BankAccount> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedAggregate writeState(BankAccount bankAccount) {
        try {
            byte[] bankAccountBytes = objectMapper.writeValueAsBytes(bankAccount);
            scala.collection.immutable.Map<String, String> map = HashMap$.MODULE$.empty();
            return SerializedAggregate.apply(bankAccountBytes, map);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
// #surge_model_class
