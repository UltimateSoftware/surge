// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

using System;

namespace Surge
{
    // ReSharper disable once ClassNeverInstantiated.Global
    // ReSharper disable once IdentifierTypo
    public class SerDeser<TS, TE, TC>
    {
        // TODO: the following functions should probably return some Try<T> type

        // ReSharper disable once IdentifierTypo
        public SerDeser()
        {
            // empty constructor
        }

        public Func<byte[], TS> DeserializeState { get; set; }

        public Func<byte[], TE> DeserializeEvent { get; set; }

        public Func<byte[], TC> DeserializeCommand { get; set; }

        public Func<TS, byte[]> SerializeState { get; set; }

        public Func<TE, byte[]> SerializeEvent { get; set; }

        public Func<TC, byte[]> SerializeCommand { get; set; }
    }
}
