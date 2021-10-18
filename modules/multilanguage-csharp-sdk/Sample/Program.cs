// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

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

            var cqrsModel = new CqrsModel<Account, BankEvent, BankCommand>
            {
                EventHandler = input =>
                {
                    Option<Account> state;
                    BankEvent bankEvent;
                    // ReSharper disable once SuggestVarOrType_Elsewhere
                    (state, bankEvent) = input;
                    var balance = state.IsSome switch
                    {
                        true => state.ToList().Head().amount,
                        _ => 0
                    };

                    return bankEvent switch
                    {
                        MoneyWithdrawn m1 => Option<Account>.Some(new Account(balance - m1.Amount)),
                        MoneyDeposited m3 => Option<Account>.Some(new Account(balance + m3.Amount)),
                        _ => Option<Account>.None
                    };
                },
                CommandHandler = input =>
                {
                    Option<Account> state;
                    BankCommand command;
                    (state, command) = input;
                
                    Lst<BankEvent> eventList;
                    // ReSharper disable once SuggestVarOrType_Elsewhere
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
                }
            };
        }
        
    }
}