using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ICSharpCode.NRefactory.CSharp.Refactoring;
using QUT.GplexBuffers;
using QUT.Gppg;

namespace Raven.Database.Indexing
{
    internal partial class LuceneQueryScanner
    {
        public bool InMethod;

        public string HandleTermInMethod()
        {
            var commaIndex = yytext.IndexOf(',');
			if (commaIndex == -1) return DiscardEscapeChar(yytext);
	        var firstTerm = DiscardEscapeChar(yytext.Substring(0, commaIndex));
            var lastCommaIndex = yytext.LastIndexOf(',');
            var newSource = yytext.Substring(commaIndex, lastCommaIndex - commaIndex);//.Replace(",", " , ");
            var rewind = yytext.Length - lastCommaIndex - 1;
            tokTxt = null;
            tokECol -= rewind;
            tokEPos -= rewind;
            buffer.Pos -= (rewind + 1);
            code = 44;
            yylloc = new LexLocation(tokLin - 1, tokCol, tokELin - 1, tokECol);
            if (lastCommaIndex != commaIndex)
            {
                var currentContext = MkBuffCtx();
                byte[] inputBuffer = System.Text.Encoding.Default.GetBytes(newSource);
                MemoryStream stream = new MemoryStream(inputBuffer);
                SetSource(stream);
                (buffer as BuildBuffer).SetPaddingOn = true;
                bStack.Push(currentContext);
            }
            //CommaPaddingStream
            return firstTerm;
        }

        public void PublicSetSource(string source)
        {
            SetSource(source, 0);
        }

        protected override bool yywrap()
        {
            if (bStack.Count == 0) return true;
            RestoreBuffCtx(bStack.Pop()); return false;
        }
        Stack<BufferContext> bStack = new Stack<BufferContext>();
		public override void yyerror(string format, params object[] args)
		{
			base.yyerror(format, args);
			Console.WriteLine(format, args);
			Console.WriteLine();
		}
		/// <summary> Returns a String where the escape char has been
		/// removed, or kept only once if there was a double escape.
		/// 
		/// Supports escaped unicode characters, e. g. translates
		/// <c>\\u0041</c> to <c>A</c>.
		/// 
		/// </summary>
		private String DiscardEscapeChar(String input)
		{
			if (input.IndexOf('\\') == -1) return input;
			// Create char array to hold unescaped char sequence
			char[] output = new char[input.Length];

			// The Length of the output can be less than the input
			// due to discarded escape chars. This variable holds
			// the actual Length of the output
			int Length = 0;

			// We remember whether the last processed character was 
			// an escape character
			bool lastCharWasEscapeChar = false;

			// The multiplier the current unicode digit must be multiplied with.
			// E. g. the first digit must be multiplied with 16^3, the second with 16^2...
			int codePointMultiplier = 0;

			// Used to calculate the codepoint of the escaped unicode character
			int codePoint = 0;

			for (int i = 0; i < input.Length; i++)
			{
				char curChar = input[i];
				if (codePointMultiplier > 0)
				{
					codePoint += HexToInt(curChar) * codePointMultiplier;
					codePointMultiplier = URShift(codePointMultiplier, 4);
					if (codePointMultiplier == 0)
					{
						output[Length++] = (char)codePoint;
						codePoint = 0;
					}
				}
				else if (lastCharWasEscapeChar)
				{
					if (curChar == 'u')
					{
						// found an escaped unicode character
						codePointMultiplier = 16 * 16 * 16;
					}
					else
					{
						// this character was escaped
						output[Length] = curChar;
						Length++;
					}
					lastCharWasEscapeChar = false;
				}
				else
				{
					if (curChar == '\\')
					{
						lastCharWasEscapeChar = true;
					}
					else
					{
						output[Length] = curChar;
						Length++;
					}
				}
			}

			if (codePointMultiplier > 0)
			{
				throw new Exception("Truncated unicode escape sequence.");
			}

			if (lastCharWasEscapeChar)
			{
				throw new Exception("Term can not end with escape character.");
			}

			return new String(output, 0, Length);
		}

		/// <summary>Returns the numeric value of the hexadecimal character </summary>
		private static int HexToInt(char c)
		{
			if ('0' <= c && c <= '9')
			{
				return c - '0';
			}
			else if ('a' <= c && c <= 'f')
			{
				return c - 'a' + 10;
			}
			else if ('A' <= c && c <= 'F')
			{
				return c - 'A' + 10;
			}
			else
			{
				throw new Exception("None-hex character in unicode escape sequence: " + c);
			}
		}
		/// <summary>
		/// Performs an unsigned bitwise right shift with the specified number
		/// </summary>
		/// <param name="number">Number to operate on</param>
		/// <param name="bits">Ammount of bits to shift</param>
		/// <returns>The resulting number from the shift operation</returns>
		public static int URShift(int number, int bits)
		{
			return (int)(((uint)number) >> bits);
		}
    }
}
