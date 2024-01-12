using System;

namespace Polar.Datanode
{
    class Program
    {
        static void Main(string[] args)
        {
            Datanode dnode = new Datanode();
            if (args.Length > 0 && args[0] != "-h")
            {
                if (args.Length == 1) dnode.Master(args[0]);
                else if (args.Length == 2) dnode.Slave(args[0], args[1]);
            }
            //string mode = dnode.Ismaster ? "master" : "slave";
            //Console.WriteLine($"Start DataNode ({mode})");

            for (; ; )
            {
                Console.Write(">>");
                string line = Console.ReadLine();
                if (string.IsNullOrEmpty(line) || line == "exit") break;
                string[] parts = line.Split(new char[] { ' ', '(', ',' }, StringSplitOptions.RemoveEmptyEntries);
                if (line == "-h")
                {
                    Console.WriteLine("(Master should be defined first)");
                    Console.WriteLine("Runs:");
                    Console.WriteLine("\t DataNode path -- for master mode");
                    Console.WriteLine("\t DataNode path masterhost -- for slave mode");
                    Console.WriteLine("\t DataNode -- for both modes");

                    Console.WriteLine("Commands:");
                    Console.WriteLine("\tmaster path -- declare master node");
                    Console.WriteLine("\tslave path masterhost -- declare slave node");
                    Console.WriteLine("\task -- send text request to master");
                    Console.WriteLine("\tasks [ntimes] -- test for multiple ask");
                    Console.WriteLine("\tcommand -- send binary request from master");
                    Console.WriteLine("\tcommands [ntimes] -- multiple command");
                    Console.WriteLine("\texit or <enter> -- for exit");
                    Console.WriteLine("\t");
                }
                else if (parts[0] == "master")
                {
                    dnode.Master(parts[1]);
                    Console.WriteLine("node set to Master mode");
                }
                else if (parts[0] == "slave")
                {
                    dnode.Slave(parts[1], parts[2]);
                    Console.WriteLine("node set to Slave mode");
                }
                else if (line == "ask") { string s = dnode.Ask(); Console.WriteLine(s); }
                else if (parts[0] == "asks")
                {
                    System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                    int nrequests = 100;
                    if (parts.Length > 1) Int32.TryParse(parts[1], out nrequests);
                    sw.Start();
                    for (int i = 0; i < nrequests; i++)
                    {
                        dnode.Ask();
                    }
                    sw.Stop();
                    Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for {nrequests} requests");
                }
                else if (line == "order")
                {
                    if (!dnode.Ismaster) { Console.WriteLine("Err: wrong order"); }
                    else
                    {
                        object ob = dnode.Order(0, 1, 77);
                        Console.WriteLine($"binary response: {ob}");
                    }
                }
                else if (parts[0] == "orders")
                {
                    System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                    int nrequests = 100;
                    if (parts.Length > 1) Int32.TryParse(parts[1], out nrequests);
                    sw.Start();
                    if (dnode.Ismaster)
                    {
                        int counter = 0;
                        for (int i = 0; i < nrequests; i++)
                        {
                            object ob = dnode.Order(0, 1, 78);
                            if (ob != null) counter++;
                        }
                        sw.Stop();
                        Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for {nrequests} requests. {counter} replies");
                    }
                }
                else if (parts[0] == "speed")
                {
                    System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                    int nrequests = 10;
                    if (parts.Length > 1) Int32.TryParse(parts[1], out nrequests);
                    sw.Start();
                    if (dnode.Ismaster)
                    {
                        for (int i = 0; i < nrequests; i++)
                        {
                            dnode.TestOrder();
                        }
                        sw.Stop();
                        Console.WriteLine($"{sw.ElapsedMilliseconds} ms. for {nrequests} requests.");
                    }
                }
                else if (parts[0] == "test3")
                {
                    int nelements = 40000;
                    if (parts.Length > 1) Int32.TryParse(parts[1], out nelements);
                    dnode.Test3(nelements);
                }
                else
                {
                    Console.WriteLine("wrong command");
                }
            }
        }
    }
}
