using System.Collections.Generic;

namespace PADIMapNoReduce {
    public interface IMapper {
        IList<KeyValuePair<string, string>> Map(string fileLine);
    }

    public interface IMapperTransfer {
        bool SendMapper(byte[] code, string className);
    }

    public interface IWorker : IMapperTransfer { }
}
