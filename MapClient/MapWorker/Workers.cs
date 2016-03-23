using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PADIMapNoReduce;
using System.Threading;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Collections;
using System.Reflection;

namespace MapWorkers {
    class Workers {

        public static MapWorker possibleJobTracker;
        public static MapWorker realJobTracker;
        public static MapWorker myRemoteObject;


        [STAThread]
        static void Main(string[] args) {

            Console.WriteLine("Worker");

            String[] parsedArgs = args[0].Split('|');

            String myUrl = parsedArgs[0];
            String possibleJobTrackerUrl = null;
            String realJobTrackerUrl = null;
            bool isJobTracker = false;

            Console.WriteLine("My url: " + myUrl);

            if (parsedArgs.Length == 3) {
                possibleJobTrackerUrl = parsedArgs[1];
            }
            else {
                //sou o meu proprio jobtracker
                isJobTracker = true;
            }

            String[] parsedURL = myUrl.Split(':');
            parsedURL = parsedURL[2].Split('/');

            String myPort = parsedURL[0];

            //registar o serviço do worker
            TcpChannel channel = new TcpChannel(Int32.Parse(myPort));
            ChannelServices.RegisterChannel(channel, true);

            RemotingConfiguration.RegisterWellKnownServiceType(
                typeof(MapWorker),
                "W",
                WellKnownObjectMode.Singleton);

            myRemoteObject = (MapWorker)Activator.GetObject(
                    typeof(MapWorker),
                    myUrl);

            myRemoteObject.setUrl(myUrl);

            if (isJobTracker) {
                //actualizo o estado do meu objecto remoto e digo que sou o jobtracker
                myRemoteObject.setJobtrackerUrl(myUrl);
                myRemoteObject.initSendAliveThread();
            }

            //se existir entryUrl, tenho de falar com ele
            if (possibleJobTrackerUrl != null) {
                possibleJobTracker = (MapWorker)Activator.GetObject(
                    typeof(MapWorker),
                    possibleJobTrackerUrl);

                realJobTrackerUrl = possibleJobTracker.getJobtrackerUrl();

                realJobTracker = (MapWorker)Activator.GetObject(
                    typeof(MapWorker),
                    realJobTrackerUrl);

                realJobTracker.registerWorker(myPort);

                //actualizo o meu jobtracker para quando falarem comigo eu redireccionar
                myRemoteObject.setJobtrackerUrl(realJobTrackerUrl);

                myRemoteObject.initTicTacThread();
            }

            System.Console.WriteLine("Press any key to exit");
            System.Console.Read();

        }

        public class MapWorker : MarshalByRefObject, IMapWorker {

            private List<List<String>> splitsPerWorker = new List<List<String>>();
            private List<String> workersUrl = new List<String>();
            private delegate void startWorkDelegate(List<String> splits, byte[] code, String className);
            private byte[] code;
            private string className;
            private int splitsNum = -1;
            private int totalLines;
            private String status;
            private String jobTrackerUrl;
            private String myUrl;
            private bool isSlow = false;
            private bool isFreezedW = false;
            private bool isFreezedC = false;
            private int delaySecs;
            private Thread ticTacThread;
            private Thread sendAliveThread;
            private int tics = 10;
            private String myState = "Running";
            private List<String> splitsProcessed = new List<String>();


            private Dictionary<String, List<String>> delegatedWork = new Dictionary<string,List<string>>();

            public void registerWorker(String port) {
                workersUrl.Add("tcp://localhost:" + port + "/W");
            }

            public void sendMapper(byte[] code, string className, int splitsNum, int totalLines) {
                delegatedWork.Clear();

                this.code = code;
                this.className = className;
                this.splitsNum = splitsNum;
                this.totalLines = totalLines;

                //definir o tamanho dos splits
                int splitSize = totalLines / splitsNum;
                int splitBegin = 1;
                int splitEnd = splitSize;
                int splitID = 0;

                int numberOfWorkers = workersUrl.Count + 1;

                for (int i = 0; i < numberOfWorkers; i++)
                    splitsPerWorker.Add(new List<String>());
          
                for (int i = 0; i < splitsNum; i++) {
                    splitsPerWorker[i % numberOfWorkers].Add(splitBegin + "|" + splitEnd + "|" + splitID++);

                    splitBegin = splitEnd + 1;
                    splitEnd += splitSize;
                }

                int j = 0;
                //mandar o jobtracker trabalhar
                startWorkDelegate jobTrackerDelegate = new startWorkDelegate(this.doWork);
                IAsyncResult LocalAr = jobTrackerDelegate.BeginInvoke(splitsPerWorker[j], code, className, null, null);

                delegatedWork.Add(myUrl, splitsPerWorker[j]);

                foreach (String url in workersUrl) {
                    j++;
                    
                    MapWorker worker = (MapWorker)Activator.GetObject(
                    typeof(MapWorker),
                    url);

                    while (isFreezedC) { }

                    //so delega trabalho se for jobtracker
                    if(myUrl.Equals(jobTrackerUrl)){
                        delegatedWork.Add(url, splitsPerWorker[j]);

                        startWorkDelegate workerDelegate = new startWorkDelegate(worker.doWork);
                        IAsyncResult RemAr = workerDelegate.BeginInvoke(splitsPerWorker[j], code, className, null, null);
                    }
                }

                splitsPerWorker.Clear();
            }

            public void doWork(List<String> splits, byte[] code, String className){
                splitsProcessed.Clear();
                IList<KeyValuePair<string, string>> finalResult = new List<KeyValuePair<string, string>>();
                Type myType = null;
                object classObj = null;

                Assembly assembly = Assembly.Load(code);

                // Walk through each type in the assembly looking for our class
                foreach (Type type in assembly.GetTypes()) {
                    if (type.IsClass == true) {
                        if (type.FullName.EndsWith("." + className)) {
                            classObj = Activator.CreateInstance(type);
                            myType = type;
                        }
                    }
                }

                IMapClient client = (IMapClient)Activator.GetObject(
                                    typeof(IMapClient),
                                    "tcp://localhost:10001/C");


                foreach (String s in splits) {
                    string[] boundaries = s.Split('|');
                    this.status = "Getting split " + boundaries[2] + " from client...";
                    String result = client.getSplit(Int32.Parse(boundaries[0]), Int32.Parse(boundaries[1]));

                    if (result != null && classObj != null && myType != null) {
                        String[] linesFromResult = result.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);
                        foreach (String line in linesFromResult) {
                            // Dynamically Invoke the method
                            object[] args = new object[] { line }; //<- cada uma das linhas retornadas pelo getSplit

                            while (isFreezedW) { }

                            this.status = "Computing split " + boundaries[2] + "...";

                            object resultObject = myType.InvokeMember("Map",
                                BindingFlags.Default | BindingFlags.InvokeMethod,
                                    null,
                                    classObj,
                                    args);
                            IList<KeyValuePair<string, string>> lineResult = (IList<KeyValuePair<string, string>>)resultObject;

                            foreach (KeyValuePair<string, string> p in lineResult)
                                finalResult.Add(p);
                        }
                    }

                    if (isSlow) {
                        Thread.Sleep(delaySecs * 1000);
                        isSlow = false;
                    }

                    this.status = "Sending split " + boundaries[2] + " to client...";
                    splitsProcessed.Add(boundaries[2]);
                    client.submitResult(finalResult, Int32.Parse(boundaries[2]), myUrl);
                }
            }

            public void printStatus() {
                Console.WriteLine(this.status);
                Console.WriteLine("******* STATO *******");
                Console.WriteLine("My URL: " + myUrl);
                Console.WriteLine("My state is: " + myState);

                //se for jobtracker
                if (jobTrackerUrl.Equals(myUrl)) {
                    Console.WriteLine("My workers:");
                    if (workersUrl.Count == 0) {
                        Console.WriteLine("There are no workers.");

                        foreach (KeyValuePair<String, List<String>> pair in delegatedWork) {
                            Console.WriteLine("*** WORKER: " + pair.Key);

                            foreach (String s in pair.Value) {
                                String[] parse = s.Split('|');
                                Console.WriteLine("Split ID: " + parse[2] + " Begin: " + parse[0] + " End: " + parse[1]);
                            }
                            Console.WriteLine();
                        }
                    }
                    else {
                        foreach (String s in workersUrl) {
                            Console.WriteLine(s);
                        }

                        if(delegatedWork.Count == 0) {
                            Console.WriteLine("No works assigned.");
                        }
                        else {
                            foreach (KeyValuePair<String, List<String>> pair in delegatedWork) {
                                Console.WriteLine("*** WORKER: " + pair.Key);

                                foreach (String s in pair.Value) {
                                    String[] parse = s.Split('|');
                                    Console.WriteLine("Split ID: " + parse[2] + " Begin: " + parse[0] + " End: " + parse[1]);
                                }
                                Console.WriteLine();
                            }
                        }
                    }
                }
                else {
                    Console.WriteLine("My JobTracker is: " + jobTrackerUrl);
                }
                Console.Write("Splits IDs already processed: ");
                foreach (String s in splitsProcessed)
                    Console.Write(s + " ");

                Console.WriteLine();
                Console.WriteLine("*********************");
            }

            public void slow(int delay) {
                isSlow = true;
                delaySecs = delay;
                myState = "Slowed.";
            }

            public void freezew() {
                isFreezedW = true;
                myState = "Freezed.";
            }

            public void unfreezew() {
                isFreezedW = false;
                myState = "Running.";
            }

            public void freezec() {
                isFreezedC = true;
                myState = "Freezed.";
                sendAliveThread.Abort();
            }

            public void unfreezec() {
                myState = "Running.";

                MapWorker newJobTracker = (MapWorker)Activator.GetObject(
                                        typeof(MapWorker),
                                        jobTrackerUrl);

                String[] parseUrl = myUrl.Split(':');
                String[] parsedPort = parseUrl[2].Split('/');
                String myPort = parsedPort[0];

                newJobTracker.registerWorker(myPort);

                isFreezedC = false;
                initTicTacThread();
            }

            public String getJobtrackerUrl() {
                return this.jobTrackerUrl;
            }

            public void setJobtrackerUrl(String jobTrackerUrl) {
                this.jobTrackerUrl = jobTrackerUrl;
            }

            public void setUrl(String myUrl) {
                this.myUrl = myUrl;
            }

            public void initTicTacThread() {
                ticTacThread = new Thread(new ThreadStart(ticTac));
                ticTacThread.Start();
            }

            private void ticTac() {
                while (true) {
                    Thread.Sleep(1000);
                    tics--;

                    if (tics == 0) {
                        Console.WriteLine("JOB TRACKER IS DEAD!!!");
                        if (workersUrl[0].Equals(myUrl)) {
                            Console.WriteLine("I'M THE NEW JOBTRACKER!!!");
                            workersUrl.Remove(myUrl);

                            MapWorker oldJobTracker = (MapWorker)Activator.GetObject(
                                            typeof(MapWorker),
                                            this.getJobtrackerUrl());

                            if (oldJobTracker != null)
                                oldJobTracker.setJobtrackerUrl(myUrl);
                            
                            this.setJobtrackerUrl(myUrl);

                            foreach (String url in workersUrl) {
                                MapWorker worker = (MapWorker)Activator.GetObject(
                                            typeof(MapWorker),
                                            url);

                                worker.setJobtrackerUrl(myUrl);
                            }

                            this.initSendAliveThread();
                            //se ja fizemos submit
                            if (splitsNum != -1)    
                                this.sendMapper(code, className, splitsNum, totalLines);
                            this.ticTacThread.Abort();
                        }
                    }
                }
            }

            public void initSendAliveThread() {
                sendAliveThread = new Thread(new ThreadStart(sendAlive));
                sendAliveThread.Start();
            }

            private void sendAlive() {

                MapWorker worker;
                while (true) {

                    Thread.Sleep(3000);
                    foreach (String url in workersUrl) {
                        worker = (MapWorker)Activator.GetObject(
                        typeof(MapWorker),
                        url);

                        worker.jobTrackerisAlive(workersUrl, code, className, splitsNum, totalLines);
                    }

                }
            }

            public void jobTrackerisAlive(List<String> workersUrlList, byte[] code, string className, int splitsNum, int totalLines) {
                this.code = code;
                this.className = className;
                this.splitsNum = splitsNum;
                this.totalLines = totalLines;
                this.workersUrl = workersUrlList;
                tics = 10;
            }
        }
    }
}