﻿using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;

using Microsoft.Diagnostics.Tracing.Session;

using NDesk.Options;

using Raven.Abstractions;
using Raven.Monitor.IO;

namespace Raven.Monitor
{
	public class Program
	{
		public static void Main(string[] args)
		{
			Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
			Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;

			if (TraceEventSession.IsElevated() == false)
			{
				Console.WriteLine("Raven.Monitor requires elevated privileges. Please run it as administrator.");
				Environment.Exit((int)ExitCodes.InsufficientPrivileges);
			}

			var program = new Program();
			program.Initialize();
			program.ParseArguments(args);

			program.AssertOptions();
		    Console.WriteLine();
			program.Execute();
		}

		private readonly MonitorOptions options = new MonitorOptions();

		private OptionSet optionSet;

		private void Initialize()
		{
			optionSet = new OptionSet();
			optionSet.Add("disk-io", OptionCategory.None, "Disk IO monitoring", _ => options.Action = MonitorActions.DiskIo);
			optionSet.Add("process-id=", OptionCategory.None, "ProcessID to monitor", processId => options.ProcessId = int.Parse(processId));
			optionSet.Add("server-url=", OptionCategory.None, "ServerUrl to RavenDB server", serverUrl => options.ServerUrl = serverUrl);
			optionSet.Add("disk-io-duration=", OptionCategory.DiskIOMonitoring, "Disk IO monitoring duration (in minutes)", duration => options.IoOptions.DurationInMinutes = int.Parse(duration));
			optionSet.Add("h|?|help", OptionCategory.Help, string.Empty, v =>
			{
				PrintUsage();
				Environment.Exit((int)ExitCodes.Success);
			});
		}

		private void AssertOptions()
		{
			switch (options.Action)
			{
				case MonitorActions.None:
					Console.WriteLine("No action selected.");
					Environment.Exit((int)ExitCodes.InvalidArguments);
					break;
				case MonitorActions.DiskIo:
					if (options.ProcessId <= 0)
					{
					    var proc = Process.GetProcessesByName("Raven.Server");
					    switch (proc.Length)
					    {
                            case 0:
					            Console.WriteLine("ProcessID (--process-id) is empty, and no Raven.Server process was found.");
					            Environment.Exit((int)ExitCodes.InvalidArguments);
                                break;
                            
                            case 1:
                                NotifyAssumption(string.Format("--process-id was not specified, assuming you meant Raven.Server process {0}", proc[0].Id));
					            options.ProcessId = proc[0].Id;
					            break;
                            default:
                                Console.WriteLine("ProcessID (--process-id) is empty, and there is more than a single Raven.Server process in the system, specify which one to use!");
                                Environment.Exit((int)ExitCodes.InvalidArguments);
					            break;
					    }
					}

			        if (string.IsNullOrEmpty(options.ServerUrl))
			        {
			            options.ServerUrl = "http://localhost:8080/";
			            bool isValid = true;
			            try
			            {
                            WebRequest.Create(options.ServerUrl +"build/version").GetResponse().Close();
			            }
			            catch (Exception)
			            {
			                isValid = false;
			            }

			            if (isValid)
			            {
			                NotifyAssumption("Asuming server url is " + options.ServerUrl);
			            }
			            else
			            {
			                Console.WriteLine("ServerUrl (--server-url) cannot be empty.");
			                Environment.Exit((int) ExitCodes.InvalidArguments);
			            }
			        }

					try
					{
						Process.GetProcessById(options.ProcessId);
					}
					catch (Exception)
					{
						Console.WriteLine("Invalid processID.");
						Environment.Exit((int)ExitCodes.InvalidArguments);
					}

					if (options.IoOptions.DurationInMinutes < 1)
					{
						Console.WriteLine("Duration (--disk-io-duration) must be at least 1.");
						Environment.Exit((int)ExitCodes.InvalidArguments);
					}
					break;
			}
		}

	    private static void NotifyAssumption(string msg)
	    {
	        var foregroundColor = Console.ForegroundColor;
	        Console.ForegroundColor = ConsoleColor.DarkRed;
	        Console.WriteLine(msg);
	        Console.ForegroundColor = foregroundColor;
	    }

	    private void Execute()
		{
			switch (options.Action)
			{
				case MonitorActions.DiskIo:
					using (var monitor = new DiskIoPerformanceMonitor(options))
						monitor.Start();
					break;
				default:
					throw new NotSupportedException(options.Action.ToString());
			}
		}

		private void ParseArguments(string[] args)
		{
			try
			{
				if (args.Length == 0)
					PrintUsage();

				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine("Could not understand arguments");
				Console.WriteLine(e.Message);
				PrintUsage();

				Environment.Exit((int)ExitCodes.InvalidArguments);
			}
		}

		private void PrintUsage()
		{
			Console.WriteLine(
				@"
Backup utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------
Command line options:", SystemTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);

			Console.WriteLine();
		}
	}
}
