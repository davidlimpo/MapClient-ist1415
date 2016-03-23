using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PADIMapNoReduce;
using System.Text.RegularExpressions;

namespace LibMapper
{
    public class WordCounter : IMapper {

        public IList<KeyValuePair<string, string>> Map(String fileLine) {
            IList<KeyValuePair<string, string>> result = new List<KeyValuePair<string, string>>();
            String[] words = fileLine.Split(' ');
            int value;

            foreach (String word in words) {

                if (!String.IsNullOrEmpty(word)) {
                    bool isEmpty = !result.Any();

                    if (isEmpty) {
                        result.Add(new KeyValuePair<string, string>(word, "" + 1));
                    }
                    else {
                        foreach (KeyValuePair<string, string> element in result) {

                            if (element.Key.Equals(word)) {
                                value = Int32.Parse(element.Value);
                                value++;
                                result.Remove(element);
                                result.Add(new KeyValuePair<string, string>(word, "" + value));
                                goto NextWord;
                            }
                        }
                        result.Add(new KeyValuePair<string, string>(word, "" + 1));   
                    }
                }
            NextWord: ;
            }
            return result;
        }
    }
}
