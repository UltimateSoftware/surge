using System;
using System.Collections.Generic;
using System.Linq;
using LanguageExt;
using LanguageExt.TypeClasses;
using static LanguageExt.Prelude;
using static LanguageExt.TypeClass;
using LanguageExt.ClassInstances;

namespace Surge.Sample
{
    public class Program
    {
        
        static void Main(string[] args)
        {

            var cqrsModel = new CqrsModel<Account, BankEvent, BankCommand>();
            cqrsModel.CommandHandler = input =>
            {
                Option<Account> state;
                BankCommand command;
                (state, command) = input;
                
                Lst<BankEvent> eventList;
                Either<string, Lst<BankEvent>> result = Either<string, Lst<BankEvent>>.Bottom;
                switch (state.IsNone)
                {
                    case true:
                        switch (command)
                        {
                            case Deposit d:
                                eventList = new Lst<BankEvent>
                                {
                                    new MoneyDeposited
                                    {
                                        Amount = d.Amount
                                    }
                                };
                                result = Right<string, Lst<BankEvent>>(eventList);
                                break;
                            case Withdraw w:
                                result = Left<string, Lst<BankEvent>>("You don't have an account so you can't withdraw money!");
                                break;
                        }
                        break;
                    case false:
                        var actualState = state.ToList().Head();
                        switch (command)
                        {
                            case Deposit d:
                                eventList = new Lst<BankEvent>
                                {
                                    new MoneyDeposited
                                    {
                                        Amount = d.Amount
                                    }
                                };
                                result = Right<string, Lst<BankEvent>>(eventList);
                                break;
                            case Withdraw w:
                                if (w.Amount <= actualState.amount)
                                {
                                    eventList = new Lst<BankEvent>
                                    {
                                        new MoneyWithdrawn() 
                                        {
                                            Amount = w.Amount
                                        }
                                    };
                                    result = Right<string, Lst<BankEvent>>(eventList);
                                }
                                else
                                {
                                    result = Left<string, Lst<BankEvent>>("You don't have enough money!");
                                }
                                break;
                        }
                        break;
                }

                return result;
            };

        }
        
    }
}