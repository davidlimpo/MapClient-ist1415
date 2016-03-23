using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PADIMapNoReduce;
using System.Collections;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.RegularExpressions;

namespace MapClient {

    class ClientCore {
        public static String clientUrl = "tcp://localhost:10001/C";
        public static IMapWorker possibleJobTracker;
        public static IMapWorker realJobTracker;
        public static TcpChannel channel;


        private delegate void sendMapperDelegate(byte[] code, string className, int splitsNum, int totalLines);


        public void INIT(String possibleJobTrackerUrl) {
            channel = new TcpChannel(10001);
            ChannelServices.RegisterChannel(channel, true);

            RemotingConfiguration.RegisterWellKnownServiceType(
                typeof(MapClient),
                "C",
                WellKnownObjectMode.Singleton);

            possibleJobTracker = (IMapWorker)Activator.GetObject(
                    typeof(IMapWorker),
                    possibleJobTrackerUrl);
            String realJobTrackerUrl = possibleJobTracker.getJobtrackerUrl();

            realJobTracker = (IMapWorker)Activator.GetObject(
                typeof(IMapWorker),
                realJobTrackerUrl);
        }

        public void SUBMIT(String inputFile, int splitsNum, String outputFile, String mapName, String classPath) {
            Console.WriteLine("SUBMIT");

            int totalLines = lineCounter(inputFile);
            
            MapClient client = (MapClient)Activator.GetObject(
                    typeof(MapClient),
                    clientUrl);

            client.init(inputFile, outputFile);

            byte[] code = File.ReadAllBytes(classPath);

            sendMapperDelegate sendMapperDelegate = new sendMapperDelegate(realJobTracker.sendMapper);
            IAsyncResult LocalAr = sendMapperDelegate.BeginInvoke(code, mapName, splitsNum, totalLines, null, null);
        }

        public static int lineCounter(String inputFile) {
            StreamReader r = new StreamReader(inputFile);
            int i = 0;
            while (r.ReadLine() != null) {
                i++;
            }
            return i;
        }
    }

    public class MapClient : MarshalByRefObject, IMapClient {
        private String inputFile;
        private String outputFile;
        public IMapWorker jobTracker;
        private delegate void sendMapperDelegate(byte[] code, string className, int splitsNum, int totalLines);

        public String getSplit(int splitBegin, int splitEnd) {
            StreamReader r = new StreamReader(this.inputFile);
            int currentLineNumber = 1;
            String currenteLineValue = r.ReadLine();
            String result = "";

            while (currenteLineValue != null) {

                String parsedLine = Regex.Replace(currenteLineValue, "[^a-zA-Z0-9% _]", String.Empty);

                if (currentLineNumber >= splitBegin && currentLineNumber < splitEnd)
                    result = result + parsedLine + Environment.NewLine;

                if (currentLineNumber == splitEnd)
                    result = result + parsedLine;

                if (currentLineNumber > splitEnd)
                    break;

                currentLineNumber++;
                currenteLineValue = r.ReadLine();
            }
            return result;
        }

        public void submitResult(IList<KeyValuePair<string, string>> result, int splitId, String workerUrl) {

            TextWriter tw = new StreamWriter(this.outputFile + "\\" + splitId + ".out");

            foreach (KeyValuePair<string, string> p in result)
                tw.WriteLine(p.Key + " : " + p.Value);

            Console.WriteLine(splitId + ".out written successfully. Proccessed by " + workerUrl);
            tw.Close();
        }

        public void init(String inputFile, String outputFile) {
            this.inputFile = inputFile;
            this.outputFile = outputFile;
        }

        public void sendSubmitArgs(String entryUrl, String inputFile, String outputFile, String splitsNum, String className, String classPath){
            Console.WriteLine("SUBMIT");

            ClientCore.possibleJobTracker = (IMapWorker)Activator.GetObject(
                    typeof(IMapWorker),
                    entryUrl);

            String realJobTrackerUrl = ClientCore.possibleJobTracker.getJobtrackerUrl();

            ClientCore.realJobTracker = (IMapWorker)Activator.GetObject(
                typeof(IMapWorker),
                realJobTrackerUrl);

            int totalLines = ClientCore.lineCounter(inputFile);
            this.init(inputFile, outputFile);
            byte[] code = File.ReadAllBytes(classPath);

            sendMapperDelegate sendMapperDelegate = new sendMapperDelegate(ClientCore.realJobTracker.sendMapper);
            IAsyncResult LocalAr = sendMapperDelegate.BeginInvoke(code, className, Int32.Parse(splitsNum), totalLines, null, null);
        }
    }
}