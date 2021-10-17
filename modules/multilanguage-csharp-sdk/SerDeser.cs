using System;

namespace Surge
{
    public class SerDeser<TS, TE, TC>
    {
        // TODO: the following functions should probably return some Try<T> type

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