// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using LanguageExt;
using Newtonsoft.Json;
using static LanguageExt.Prelude;

namespace Surge.Sample
{
    public class Program
    {
        static void Main(string[] args)
        {
            var serDer = new SerDeser<Account, BankEvent, BankCommand>
            {
                DeserializeCommand = bytes =>
                {
                    var bytesAsString = Encoding.UTF8.GetString(bytes);
                    var result = JsonConvert.DeserializeObject<BankCommand>(bytesAsString);
                    return result;
                },
                DeserializeEvent = bytes =>
                {
                    var bytesAsString = Encoding.UTF8.GetString(bytes);
                    var result = JsonConvert.DeserializeObject<BankEvent>(bytesAsString);
                    return result;
                },
                DeserializeState = bytes =>
                {
                    var bytesAsString = Encoding.UTF8.GetString(bytes);
                    var result = JsonConvert.DeserializeObject<Account>(bytesAsString);
                    return result;
                },
                SerializeCommand = command =>
                {
                    // ReSharper disable once BuiltInTypeReferenceStyle
                    String jsonString = JsonConvert.SerializeObject(command);
                    var result = Encoding.UTF8.GetBytes(jsonString);
                    return result;
                },
                SerializeEvent = bankEvent =>
                {
                    String jsonString = JsonConvert.SerializeObject(bankEvent);
                    var result = Encoding.UTF8.GetBytes(jsonString);
                    return result;
                },
                SerializeState = state =>
                {
                    String jsonString = JsonConvert.SerializeObject(state);
                    var result = Encoding.UTF8.GetBytes(jsonString);
                    return result;
                }
            };
            
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

                    List<BankEvent> eventList = new List<BankEvent>();
                    // ReSharper disable once SuggestVarOrType_Elsewhere
                    Either<string, List<BankEvent>> result = Either<string, List<BankEvent>>.Bottom;
                    switch (state.IsNone)
                    {
                        case true:
                            switch (command)
                            {
                                case Deposit d:
                                    eventList.Add(
                                        new MoneyDeposited
                                        {
                                            Amount = d.Amount
                                        }
                                    );
                                    result = Right<string, List<BankEvent>>(eventList);
                                    break;
                                case Withdraw w:
                                    result = Left<string, List<BankEvent>>(
                                        "You don't have an account so you can't withdraw money!");
                                    break;
                            }

                            break;
                        case false:
                            var actualState = state.ToList().Head();
                            switch (command)
                            {
                                case Deposit d:
                                    eventList.Add( 
                                    
                                        new MoneyDeposited
                                        {
                                            Amount = d.Amount
                                        }
                                    );
                                    result = Right<string, List<BankEvent>>(eventList);
                                    break;
                                case Withdraw w:
                                    if (w.Amount <= actualState.amount)
                                    {
                                        eventList.Add(
                                            new MoneyWithdrawn()
                                            {
                                                Amount = w.Amount
                                            }
                                        );
                                        result = Right<string, List<BankEvent>>(eventList);
                                    }
                                    else
                                    {
                                        result = Left<string, List<BankEvent>>("You don't have enough money!");
                                    }

                                    break;
                            }

                            break;
                    }

                    return result;
                }
            };

            var surge = new SurgeEngine<Account, BankEvent, BankCommand>(serDer, cqrsModel);
            
            // sleeping for a few seconds
            Thread.Sleep(10000);

            
            var randomGuid = Guid.NewGuid();
            var resultA = surge.ForwardCommand(randomGuid, new Deposit()
            {
                Amount = 50
            });
            var resultB = resultA.Map
            (
                item =>
                {
                    Console.WriteLine("Option<Account> ..is defined ? " + item.IsSome);
                   foreach (var account in item)
                   {
                       Console.WriteLine("Account balance is: " + account.amount);
                   } 
                    return item;
                }
            );
           
            
            resultB.Wait();
           
            ManualResetEvent manualResetEvent = new ManualResetEvent(false);
            manualResetEvent.WaitOne();

        }
    }
}