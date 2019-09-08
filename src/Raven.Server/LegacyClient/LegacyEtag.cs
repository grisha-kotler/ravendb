using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Raven.Server.LegacyClient
{
    public class LegacyEtag
    {
        private readonly long _restarts;

        public long Etag { get; }

        public LegacyEtag(string str)
        {
            Etag = ParseEtagChanges(str);
            _restarts = 0;
        }

        public LegacyEtag(long changes, long restarts = 0)
        {
            Etag = changes;
            _restarts = restarts;
        }

        private static long ParseEtagChanges(string str)
        {
            if (string.IsNullOrEmpty(str))
                throw new ArgumentException("str cannot be empty or null");

            if (str.Length != 36)
                throw new ArgumentException("str must be 36 characters");

            var buffer = new byte[16]
            {
                byte.Parse(str.Substring(16, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(14, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(11, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(9, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(6, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(4, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(2, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(0, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(34, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(32, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(30, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(28, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(26, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(24, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(21, 2), NumberStyles.HexNumber),
                byte.Parse(str.Substring(19, 2), NumberStyles.HexNumber)
            };

            var restarts = BitConverter.ToInt64(buffer, 0);
            return BitConverter.ToInt64(buffer, 8);
        }

        public override string ToString()
        {
            var sb = new StringBuilder(36);

            foreach (var by in ToBytes())
            {
                sb.Append(by.ToString("X2"));
            }

            sb.Insert(8, "-")
                .Insert(13, "-")
                .Insert(18, "-")
                .Insert(23, "-");

            return sb.ToString();
        }

        private IEnumerable<byte> ToBytes()
        {
            foreach (var source in BitConverter.GetBytes(_restarts).Reverse())
            {
                yield return source;
            }

            foreach (var source in BitConverter.GetBytes(Etag).Reverse())
            {
                yield return source;
            }
        }

        private static readonly int[] AsciisOfHexToNum = CreateHexCharsToNumsTable();

        private static int[] CreateHexCharsToNumsTable()
        {
            var c = new int['z' + 1];
            for (var i = '0'; i <= '9'; i++)
            {
                c[i] = (char)(i - '0');
            }
            for (var i = 'A'; i <= 'Z'; i++)
            {
                c[i] = (char)((i - 'A') + 10);
            }
            for (var i = 'a'; i <= 'z'; i++)
            {
                c[i] = (char)((i - 'a') + 10);
            }

            return c;
        }

        public static unsafe LegacyEtag Parse(string str)
        {
            if (str == null || str.Length != 36)
                throw new ArgumentException("str cannot be empty or null");

            fixed (char* input = str)
            {
                int fst = ((byte)(AsciisOfHexToNum[input[0]] * 16 + AsciisOfHexToNum[input[1]])) << 24 |
                    ((byte)(AsciisOfHexToNum[input[2]] * 16 + AsciisOfHexToNum[input[3]])) << 16 |
                    ((byte)(AsciisOfHexToNum[input[4]] * 16 + AsciisOfHexToNum[input[5]])) << 8 |
                    (byte)(AsciisOfHexToNum[input[6]] * 16 + AsciisOfHexToNum[input[7]]);
                int snd = ((byte)(AsciisOfHexToNum[input[9]] * 16 + AsciisOfHexToNum[input[10]])) << 24 |
                    ((byte)(AsciisOfHexToNum[input[11]] * 16 + AsciisOfHexToNum[input[12]])) << 16 |
                    ((byte)(AsciisOfHexToNum[input[14]] * 16 + AsciisOfHexToNum[input[15]])) << 8 |
                    ((byte)(AsciisOfHexToNum[input[16]] * 16 + AsciisOfHexToNum[input[17]]));
                var restarts = (uint)snd | ((long)fst << 32);


                fst = ((byte)(AsciisOfHexToNum[input[19]] * 16 + AsciisOfHexToNum[input[20]])) << 24 |
                    ((byte)(AsciisOfHexToNum[input[21]] * 16 + AsciisOfHexToNum[input[22]])) << 16 |
                    ((byte)(AsciisOfHexToNum[input[24]] * 16 + AsciisOfHexToNum[input[25]])) << 8 |
                    ((byte)(AsciisOfHexToNum[input[26]] * 16 + AsciisOfHexToNum[input[27]]));
                snd = ((byte)(AsciisOfHexToNum[input[28]] * 16 + AsciisOfHexToNum[input[29]])) << 24 |
                    ((byte)(AsciisOfHexToNum[input[30]] * 16 + AsciisOfHexToNum[input[31]])) << 16 |
                    ((byte)(AsciisOfHexToNum[input[32]] * 16 + AsciisOfHexToNum[input[33]])) << 8 |
                    ((byte)(AsciisOfHexToNum[input[34]] * 16 + AsciisOfHexToNum[input[35]]));
                var changes = (uint)snd | ((long)fst << 32);

                return new LegacyEtag(changes, restarts);
            }
        }

        public static unsafe LegacyEtag Parse(byte[] bytes)
        {
            if (bytes.Length == 0)
                throw new InvalidOperationException("Etag is not valid, bytes is zero");

            fixed (byte* restarts = bytes)
            {
                int fst = (*restarts << 24) | (*(restarts + 1) << 16) | (*(restarts + 2) << 8) | (*(restarts + 3));
                int snd = (*(restarts + 4) << 24) | (*(restarts + 5) << 16) | (*(restarts + 6) << 8) | (*(restarts + 7));
                var etagRestarts = (uint)snd | ((long)fst << 32);

                var changes = restarts + 8;

                fst = (*changes << 24) | (*(changes + 1) << 16) | (*(changes + 2) << 8) | (*(changes + 3));
                snd = (*(changes + 4) << 24) | (*(changes + 5) << 16) | (*(changes + 6) << 8) | (*(changes + 7));
                var etagChanges = (uint)snd | ((long)fst << 32);

                return new LegacyEtag(etagChanges, etagRestarts);
            }
        }

        public byte[] ToByteArray()
        {
            return ToBytes().ToArray();
        }

        public static LegacyEtag InvalidEtag
        {
            get => new LegacyEtag(-1, -1);
        }
    }
}
