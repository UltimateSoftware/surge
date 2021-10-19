// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

using JsonSubTypes;
using Newtonsoft.Json;

namespace Surge.Sample
{
    public class Account
    {
        public Account(int amount)
        {
            this.amount = amount;
        }

        public int amount { get; }
    }

    [JsonConverter(typeof(JsonSubtypes), "Type")]
    [JsonSubtypes.KnownSubTypeAttribute(typeof(MoneyWithdrawn), "Withdraw")]
    [JsonSubtypes.KnownSubTypeAttribute(typeof(MoneyDeposited), "Deposit")]
    public class BankCommand
    {
        public virtual string Type { get; }
    }

    public class Withdraw : BankCommand
    {
        public override string Type { get; } = "Withdraw";
        public int Amount { get; set; }
    }

    public class Deposit : BankCommand
    {
        public override string Type { get; } = "Deposit";
        public int Amount { get; set; }
    }


    [JsonConverter(typeof(JsonSubtypes), "Type")]
    [JsonSubtypes.KnownSubTypeAttribute(typeof(MoneyWithdrawn), "MoneyWithdrawn")]
    [JsonSubtypes.KnownSubTypeAttribute(typeof(MoneyDeposited), "MoneyDeposited")]
    public class BankEvent
    {
        public virtual string Type { get; }
    }

    public class MoneyWithdrawn : BankEvent
    {
        public override string Type { get; } = "MoneyWithdrawn";
        public int Amount { get; set; }
    }

    public class MoneyDeposited : BankEvent
    {
        public override string Type { get; } = "MoneyDeposited";
        public int Amount { get; set; }
    }
}