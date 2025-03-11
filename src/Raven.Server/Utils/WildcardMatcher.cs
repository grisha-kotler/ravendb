// -----------------------------------------------------------------------
//  <copyright file="WildcardMatcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;

namespace Raven.Server.Utils
{
    public class WildcardMatcher
    {
        public static bool Matches(PreprocessedPattern preprocessed, string input)
        {
            if (preprocessed.Patterns.Length == 0)
                return true;

            return MatchesImpl(preprocessed, input);
        }

        public static bool MatchesExclusion(PreprocessedPattern preprocessed, string input)
        {
            // empty means no match
            if (preprocessed.Patterns.Length == 0)
                return false;

            return MatchesImpl(preprocessed, input);
        }

        private static bool MatchesImpl(PreprocessedPattern preprocessed, string input)
        {
            return preprocessed.Patterns.Any(p => MatchesImpl(p.Pattern, input, 0, 0, p.MinLengths));
        }

        private static bool MatchesImpl(string pattern, string input, int patternPos, int inputPos, int[] minLengths)
        {
            if (string.IsNullOrEmpty(pattern))
                return true;

            if (input == null)
                throw new ArgumentNullException(nameof(input));

            if (input.Length == 0)
                return false;

            for (int i = inputPos; i < input.Length; i++)
            {
                if (patternPos >= pattern.Length)
                    return false; // input has more than the pattern

                if (input.Length - i < minLengths[patternPos])
                    return false; // early exit: check if remaining input is not long enough

                var currentPatternChar = pattern[patternPos];
                var currentInputChar = char.ToUpperInvariant(input[i]);

                switch (currentPatternChar)
                {
                    case '*':
                        // match a number of letters, need to check the _next_ pattern pos for a match, which will end 
                        // our current * matching
                        if (patternPos + 1 < pattern.Length)
                        {
                            var nextPatternChar = char.ToUpperInvariant(pattern[patternPos + 1]);
                            if (currentInputChar == nextPatternChar ||
                                nextPatternChar == '?')
                            { // we have a match for the next part, let us see if it is an actual match

                                if (MatchesImpl(pattern, input, patternPos + 1, i, minLengths))
                                    return true;
                            }
                        }
                        break;
                    case '?': // matches any single letter
                        patternPos++;
                        break;
                    default:
                        if (currentInputChar != currentPatternChar)
                            return false;
                        patternPos++;
                        break;
                }
            }

            return patternPos == pattern.Length ||
                   (patternPos == pattern.Length - 1 && pattern[patternPos] == '*');
        }

        public class PatternInfo
        {
            public string Pattern { get; }
            public int[] MinLengths { get; }

            public PatternInfo(string pattern)
            {
                Pattern = pattern;
                MinLengths = PrecomputeMinimumLengths(pattern);
            }

            private static int[] PrecomputeMinimumLengths(string pattern)
            {
                int n = pattern.Length;
                int[] minLengths = new int[n + 1];

                minLengths[n] = 0;

                for (int i = n - 1; i >= 0; i--)
                {
                    char c = pattern[i];
                    if (c == '*')
                    {
                        minLengths[i] = minLengths[i + 1];
                    }
                    else
                    {
                        minLengths[i] = minLengths[i + 1] + 1;
                    }
                }

                return minLengths;
            }
        }

        public class PreprocessedPattern
        {
            private static readonly char[] Separator = { '|' };

            public PatternInfo[] Patterns { get; }

            public PreprocessedPattern(string pattern)
            {
                if (string.IsNullOrEmpty(pattern))
                {
                    Patterns = [];
                    return;
                }

                var subPatterns = pattern.Split(Separator, StringSplitOptions.RemoveEmptyEntries);
                Patterns = subPatterns.Select(p => new PatternInfo(p.ToUpperInvariant())).ToArray();
            }
        }
    }
}
