using System;
using LanguageExt;

namespace Surge
{
    public class CqrsModel<S, E, C>
    {
        private Func<Tuple<Option<S>, E>, Option<S>> eventHandler;

        private Func<Tuple<Option<S>, C>, Either<String, Lst<E>>> commandHandler;


        public CqrsModel(Func<Tuple<Option<S>, E>, Option<S>> eventHandler,
            Func<Tuple<Option<S>, C>, Either<string, Lst<E>>> commandHandler)
        {
            this.eventHandler = eventHandler ?? throw new ArgumentNullException(nameof(eventHandler));
            this.commandHandler = commandHandler ?? throw new ArgumentNullException(nameof(commandHandler));
        }
    }
}