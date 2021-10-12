using System;
using LanguageExt;

namespace Surge
{
    public class CqrsModel<TS, TE, TC>
    {
        public CqrsModel()
        {
            // empty constructor
        }

        private Func<Tuple<Option<TS>, TE>, Option<TS>> EventHandler { get; set; }

        private Func<Tuple<Option<TS>, TC>, Either<string, Lst<TE>>> CommandHandler { get; set; }
    }
}
