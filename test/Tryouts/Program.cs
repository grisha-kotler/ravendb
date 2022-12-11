using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using RachisTests;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Voron;
using StressTests.Issues;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 10_000; i++)
            {
                 Console.WriteLine($"Starting to run {i}");
                try
                {
                    var dictionary = new Dictionary<string, int>();
                    dictionary.Add("a", 3);
                    dictionary.Add("c", 3);
                    dictionary.Add("d", 3);

                    /*foreach (var VARIABLE in new HashSet<string>(dictionary.Keys))
                    {
                        dictionary.Add(new string('a', 3), 3);
                    }*/

                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    //using (var test = new RavenDB_19016(testOutputHelper))
                    using (var test = new MultiAdds(testOutputHelper))
                    {
                        //await test.Can_Index_Nested_Document_Change();
                        //await test.Can_Index_Nested_CompareExchange_Change();
                        test.MultiAdds_And_MultiDeletes_After_Causing_PageSplit_DoNot_Fail();


                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
    }
}
