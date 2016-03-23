using PADIMapNoReduce;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.ComponentModel;

namespace PuppetMaster {
    class PuppetMasters {

        //classe responsável por guardar o id e o url dos workers do puppet
        //O id é necessario para fazermos o freeze etc..
        //O url é necessário para acedermos aos objectos remotos respectivos
        public class WorkerInfo {
            private String id;
            private String url;

            public WorkerInfo(String id, String url) {
                this.id = id;
                this.url = url;
            }

            public String getId() { 
                return this.id; 
            }

            public String getUrl() {
                return this.url; 
            }
        }

        //lista com os urls de todos os puppets
        public static List<String> puppetsUrl = new List<String>();
        
        //lista com os urls de todos os workers
        public static List<WorkerInfo> workersUrl = new List<WorkerInfo>();

        private static String puppetUrl;
        private static String arguments;
        private static String clientUrl = "tcp://localhost:10001/C";
        private static MapPuppetMaster thisPuppet;
        private delegate void slowDelegate(int delay);
        private delegate void freezewDelegate();
        private delegate void unfreezewDelegate();
        private delegate void freezecDelegate();
        private delegate void unfreezecDelegate();



        static void Main(string[] args) {
            Console.WriteLine("PuppetMaster\r\nINSERT MY PORT:");

            String puppetPort = Console.ReadLine();
            puppetUrl = "tcp://localhost:" + puppetPort + "/PM";

            Console.WriteLine("HOW MANY OTHER PUPPETS?");
            String numPuppets = Console.ReadLine();

            //se existirem mais puppets, vamos guardá-los
            if (Int32.Parse(numPuppets) > 0) {
                Console.WriteLine("INSERT OTHER PUPPETMASTER PORTS:");
                String otherPuppetsPorts = Console.ReadLine();
                String[] parsedOtherPuppetPorts = otherPuppetsPorts.Split(' ');
                for (int i = 0; i < parsedOtherPuppetPorts.Length; i++) {
                    puppetsUrl.Add("tcp://localhost:" + parsedOtherPuppetPorts[i] + "/PM");
                }

            }

            //registamos o serviço do puppetmaster
            TcpChannel channel = new TcpChannel(Int32.Parse(puppetPort));
            ChannelServices.RegisterChannel(channel, true);

            RemotingConfiguration.RegisterWellKnownServiceType(
                typeof(MapPuppetMaster),
                "PM",
                WellKnownObjectMode.Singleton);

            thisPuppet = (MapPuppetMaster)Activator.GetObject(
                    typeof(MapPuppetMaster),
                    puppetUrl);

            while (true) {
                String command = Console.ReadLine();
                if (command != null) {
                    String[] parsedCommand = command.Split(' ');

                    switch (parsedCommand[0]) {
                        case "DEBUG":
                            IMapWorker worker = (IMapWorker)Activator.GetObject(
                                                    typeof(IMapWorker),
                                                    parsedCommand[1]);
                            break;
                        case "WORKER":
                            //se tivermos entryurl, entao o worker tem de contactá-lo 
                            //para descobrir o endereço do jobtracker para se registar
                            if (parsedCommand.Length == 4) {
                                //serviceurl
                                arguments = parsedCommand[3] + "|";
                            }
                            //senão é porque somos o primeiro e assumimos o papel de jobtracker.
                            else {
                                //serviceurl + entryurl
                                arguments = parsedCommand[3] + "|" + parsedCommand[4] + "|";
                            }

                            //se for neste puppet
                            if (parsedCommand[2].CompareTo(puppetUrl) == 0) {
                                //guarda na lista o id e o url
                                workersUrl.Add(new WorkerInfo(parsedCommand[1], parsedCommand[3]));
                                Process.Start("..\\..\\..\\MapWorker\\bin\\Debug\\MapWorker.exe", arguments);
                            }
                            //se for noutro puppet, vamos buscá-lo e siga, mas neste caso o worker é adicionado à lista do mesmo
                            else {
                                MapPuppetMaster puppet = (MapPuppetMaster)Activator.GetObject(
                                                        typeof(MapPuppetMaster),
                                                        parsedCommand[2]);
                                //enviamos o id, o serviceurl e o entryurl se existir
                                puppet.PuppetMasterURL(parsedCommand[1], arguments);
                            }

                            Console.WriteLine("Worker created on " + parsedCommand[2]);
                            break;
                        case "SUBMIT":

                            //se existirem outros puppets, vamos ver se algum registou o cliente
                            if (puppetsUrl.Count > 0) {
                                foreach (String url in puppetsUrl) {
                                    MapPuppetMaster otherPuppet = (MapPuppetMaster)Activator.GetObject(
                                                            typeof(MapPuppetMaster),
                                                            url);

                                    //se algum o tiver registado, actualizo-me
                                    if(otherPuppet.alreadyRegisteredClient){
                                        thisPuppet.alreadyRegisteredClient = true;
                                    }
                                }
                            }

                            if(thisPuppet.alreadyRegisteredClient){
                                IMapClient client = (IMapClient)Activator.GetObject(
                                    typeof(IMapClient),
                                    clientUrl);
                                client.sendSubmitArgs(parsedCommand[1], parsedCommand[2], parsedCommand[3], parsedCommand[4], parsedCommand[5], parsedCommand[6]);
                            }
                            else{
                                arguments = parsedCommand[1] + "|" + parsedCommand[2] + "|" + parsedCommand[3] + "|" + parsedCommand[4] + "|" + parsedCommand[5] + "|" + parsedCommand[6];
                                thisPuppet.alreadyRegisteredClient = true;
                                Process.Start("..\\..\\..\\MapClient\\bin\\Debug\\MapClient.exe", arguments);
                            }
                        
                            break;
                        case "WAIT":

                            Console.WriteLine("Waiting for " + parsedCommand[1] + " seconds...");
                            System.Threading.Thread.Sleep(Int32.Parse(parsedCommand[1]) * 1000);
                            
                            break;
                        case "STATUS":

                            foreach (WorkerInfo w in workersUrl) {
                                worker = (IMapWorker)Activator.GetObject(
                                    typeof(IMapWorker),
                                    w.getUrl());

                                worker.printStatus();
                            }

                            if (puppetsUrl.Count > 0) {
                                foreach (String url in puppetsUrl) {
                                    MapPuppetMaster puppet = (MapPuppetMaster)Activator.GetObject(
                                                            typeof(MapPuppetMaster),
                                                            url);

                                    puppet.printStatus();
                                }
                            }

                            break;
                        case "SLOWW":
                            foreach (WorkerInfo w in workersUrl) {
                                if (w.getId().Equals(parsedCommand[1])) {
                                    worker = (IMapWorker)Activator.GetObject(
                                    typeof(IMapWorker),
                                    w.getUrl());

                                    slowDelegate slow = new slowDelegate(worker.slow);
                                    IAsyncResult LocalAr = slow.BeginInvoke(Int32.Parse(parsedCommand[2]), null, null);

                                }
                            }

                            break;
                        case "FREEZEW":

                            foreach (WorkerInfo w in workersUrl) {
                                if (w.getId().Equals(parsedCommand[1])) {
                                    worker = (IMapWorker)Activator.GetObject(
                                    typeof(IMapWorker),
                                    w.getUrl());

                                    freezewDelegate freezew = new freezewDelegate(worker.freezew);
                                    IAsyncResult LocalAr = freezew.BeginInvoke(null, null);
                                }
                            }

                            break;
                        case "UNFREEZEW":

                            foreach (WorkerInfo w in workersUrl) {
                                if (w.getId().Equals(parsedCommand[1])) {
                                    worker = (IMapWorker)Activator.GetObject(
                                    typeof(IMapWorker),
                                    w.getUrl());

                                    unfreezewDelegate unfreezew = new unfreezewDelegate(worker.unfreezew);
                                    IAsyncResult LocalAr = unfreezew.BeginInvoke(null, null);
                                }
                            }

                            break;
                        case "FREEZEC":

                            foreach (WorkerInfo w in workersUrl) {
                                if (w.getId().Equals(parsedCommand[1])) {
                                    worker = (IMapWorker)Activator.GetObject(
                                    typeof(IMapWorker),
                                    w.getUrl());

                                    freezecDelegate freezec = new freezecDelegate(worker.freezec);
                                    IAsyncResult LocalAr = freezec.BeginInvoke(null, null);
                                }
                            }

                            break;
                        case "UNFREEZEC":

                            foreach (WorkerInfo w in workersUrl) {
                                if (w.getId().Equals(parsedCommand[1])) {
                                    worker = (IMapWorker)Activator.GetObject(
                                    typeof(IMapWorker),
                                    w.getUrl());

                                    unfreezecDelegate unfreezec = new unfreezecDelegate(worker.unfreezec);
                                    IAsyncResult LocalAr = unfreezec.BeginInvoke(null, null);
                                }
                            }

                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    public class MapPuppetMaster : MarshalByRefObject, IPuppetMaster {

        public bool alreadyRegisteredClient = false;

        public void PuppetMasterURL(String id, String urlAndEntry) {
            String[] parsedArguments = urlAndEntry.Split('|');
            //guarda na lista o id e o url
            PuppetMasters.workersUrl.Add(new PuppetMasters.WorkerInfo(id, parsedArguments[0]));
            Process.Start("..\\..\\..\\MapWorker\\bin\\Debug\\MapWorker.exe", urlAndEntry);
        }

        public void printStatus() {
            foreach (PuppetMasters.WorkerInfo w in PuppetMasters.workersUrl) {
                IMapWorker worker = (IMapWorker)Activator.GetObject(
                    typeof(IMapWorker),
                    w.getUrl());

                worker.printStatus();
            }
        }
    }
}
