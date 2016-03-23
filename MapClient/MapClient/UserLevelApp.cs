using PADIMapNoReduce;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MapClient {

    class UserLevelApp {
        static void Main(string[] args) {
        ClientCore client = new ClientCore();
        
        Console.WriteLine("USER APP");

        String[] parsedArgs = args[0].Split('|');

        client.INIT(parsedArgs[0]);

        String inputFile = parsedArgs[1];
        String outputFile = parsedArgs[2];
        int numSplits = int.Parse(parsedArgs[3]);
        String mapName = parsedArgs[4];
        String classPath = parsedArgs[5];

        client.SUBMIT(inputFile, numSplits, outputFile, mapName, classPath);

        Console.ReadKey();
        }
    }
}
