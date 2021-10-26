// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

using System;
using System.Collections.Generic;
using LanguageExt;

namespace Surge
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class CqrsModel<TS, TE, TC>
    {
        public CqrsModel()
        {
            // empty constructor
        }

        public Func<Tuple<Option<TS>, TE>, Option<TS>> EventHandler { get; set; }

        public Func<Tuple<Option<TS>, TC>, Either<string, List<TE>>> CommandHandler { get; set; }
    }
}
