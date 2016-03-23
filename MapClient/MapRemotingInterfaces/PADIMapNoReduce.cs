using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace PADIMapNoReduce {

 /*   public interface IMapper {
        IList<KeyValuePair<string, string>> Map(string fileLine);
    }*/

    public interface IMapClient {
        String getSplit(int splitBegin, int splitEnd);
        void submitResult(IList<KeyValuePair<string, string>> result, int splitId, String workerUrl);
        void init(String inputFile, String outputFile);
        void sendSubmitArgs(String entryUrl, String inputFile, String outputFile, String splitsNum, String className, String classPath);
    }

    public interface IMapWorker {
        void registerWorker(String port);
        void sendMapper(byte[] code, string className, int splitsNum, int totalLines);
        void doWork(List<String> splits, byte[] code, String className);
        void printStatus();
        void slow(int delay);
        void setJobtrackerUrl(String jobTrackerUrl);
        String getJobtrackerUrl();
        void freezew();
        void unfreezew();
        void freezec();
        void unfreezec();
        void jobTrackerisAlive(List<String> workersUrlList, byte[] code, string className, int splitsNum, int totalLines);
        void setUrl(String myUrl);
        void initTicTacThread();
    }

    public interface IPuppetMaster {
        void PuppetMasterURL(String ID, String urlAndEntry);
    }
}